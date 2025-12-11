#!/usr/bin/env python3
# coding: utf-8

"""
Telegram warehouse acceptance bot (aiogram 3.x)
- Single Google Sheets worksheet used.
- Collectors and workers lists are stored as JSON in specific cells of the same worksheet.
- Date format for box date: DD-MM-YYYY.
"""

import os
import logging
import asyncio
import csv
import json
from datetime import datetime, date
from typing import List, Optional, Tuple

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import (
    ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove,
    InlineKeyboardMarkup, InlineKeyboardButton, InputFile
)
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from aiogram.filters.callback_data import CallbackData

import gspread
from oauth2client.service_account import ServiceAccountCredentials
from dotenv import load_dotenv

load_dotenv()

# ---------- Logging ----------
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# ---------- Env / Config ----------
BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_IDS = [int(x.strip()) for x in os.getenv("ADMIN_ID", "").split(",") if x.strip()]
SHEET_KEY_OR_URL = os.getenv("SHEET_KEY_OR_URL")
GOOGLE_CREDS_PATH = os.getenv("GOOGLE_CREDS_PATH", "./gservice.json")

if not BOT_TOKEN or not ADMIN_IDS or not SHEET_KEY_OR_URL:
    logger.error("Missing required env vars: BOT_TOKEN, ADMIN_ID, SHEET_KEY_OR_URL")
    raise SystemExit("Set BOT_TOKEN, ADMIN_ID and SHEET_KEY_OR_URL in .env")

bot = Bot(BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())

# ---------- Google Sheets helper (single worksheet) ----------
class GSHelper:
    """
    Single worksheet layout:
    A1: "__META__" (marker)
    A2: "COLLECTORS"   B2: JSON string -> [{"tgid":123,"name":"Anna"}, ...]
    A3: "WORKERS"      B3: JSON string -> [123,456,...]
    A4: "" (empty)
    A5.. header row: BoxID, Timestamp, PhotoFileIDs, CollectorTGID, CollectorName, Date(DD-MM-YYYY), Destination, Status, ProcessedByTGID, ProcessedAt, Notes
    Rows after header = boxes
    """

    def __init__(self, creds_path: str, sheet_key_or_url: str):
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name(creds_path, scope)
        self.client = gspread.authorize(creds)
        self.sheet = self.client.open_by_url(sheet_key_or_url) if sheet_key_or_url.startswith("http") else self.client.open_by_key(sheet_key_or_url)
        # use single worksheet named "boxes"
        self._ensure_worksheet()

    def _ensure_worksheet(self):
        try:
            self.ws = self.sheet.worksheet("boxes")
        except gspread.WorksheetNotFound:
            self.ws = self.sheet.add_worksheet(title="boxes", rows="2000", cols="20")
            # Set metadata and header
            self.ws.update("A1", "__META__")
            self.ws.update("A2", "COLLECTORS")
            self.ws.update("B2", "[]")
            self.ws.update("A3", "WORKERS")
            self.ws.update("B3", "[]")
            # leave A4 empty, then headers at row 5
            headers = ["BoxID","Timestamp","PhotoFileIDs","CollectorTGID","CollectorName","Date","Destination","Status","ProcessedByTGID","ProcessedAt","Notes"]
            self.ws.append_row(headers)

    # ---- metadata (collectors/workers) ----
    def _read_json_cell(self, row_label: str) -> Optional[list]:
        # row_label = "COLLECTORS" or "WORKERS": find cell in column A then read column B value
        try:
            cell = self.ws.find(row_label)
            val = self.ws.cell(cell.row, cell.col + 1).value  # B cell
            if not val:
                return []
            return json.loads(val)
        except Exception:
            return []

    def _write_json_cell(self, row_label: str, data):
        try:
            cell = self.ws.find(row_label)
            # write JSON into B column
            self.ws.update_cell(cell.row, cell.col + 1, json.dumps(data, ensure_ascii=False))
            return True
        except Exception as e:
            logger.exception("Failed to write metadata cell: %s", e)
            return False

    def get_collectors(self) -> List[Tuple[int,str]]:
        # stored as list of objects {"tgid": int, "name": str}
        arr = self._read_json_cell("COLLECTORS")
        out = []
        for it in arr:
            try:
                out.append((int(it.get("tgid")), it.get("name","")))
            except Exception:
                continue
        return out

    def get_workers(self) -> List[int]:
        arr = self._read_json_cell("WORKERS")
        try:
            return [int(x) for x in arr]
        except Exception:
            return []

    def add_collector(self, tgid: int, name: str):
        arr = self._read_json_cell("COLLECTORS") or []
        # avoid duplicates
        for it in arr:
            if int(it.get("tgid")) == tgid:
                return False
        arr.append({"tgid": tgid, "name": name})
        return self._write_json_cell("COLLECTORS", arr)

    def add_worker(self, tgid: int):
        arr = self._read_json_cell("WORKERS") or []
        if int(tgid) in [int(x) for x in arr]:
            return False
        arr.append(int(tgid))
        return self._write_json_cell("WORKERS", arr)

    # ---- boxes management ----
    def _header_row_index(self) -> int:
        # find header row (where first column equals 'BoxID')
        try:
            cell = self.ws.find("BoxID")
            return cell.row
        except Exception:
            # fallback: assume row 5
            return 5

    def _next_box_id(self) -> str:
        # Find last BoxID value in column A after header
        hdr = self._header_row_index()
        vals = self.ws.col_values(1)[hdr:]  # after header
        if not vals:
            return "B0001"
        last = vals[-1]
        if not last or not last.startswith("B"):
            return "B0001"
        try:
            n = int(last.lstrip("B")) + 1
            return f"B{n:04d}"
        except Exception:
            return f"B{len(vals)+1:04d}"

    def add_box(self, photo_file_ids: List[str], collector_tgid: int, collector_name: str, box_date_ddmmyyyy: str, destination: str, notes: str="") -> str:
        boxid = self._next_box_id()
        ts = datetime.utcnow().isoformat()
        row = [
            boxid,
            ts,
            "|".join(photo_file_ids),
            str(collector_tgid),
            collector_name,
            box_date_ddmmyyyy,
            destination,
            "Новая",
            "",
            "",
            notes
        ]
        self.ws.append_row(row)
        logger.info("Added box %s", boxid)
        return boxid

    def find_box_row(self, boxid: str) -> Optional[int]:
        try:
            cell = self.ws.find(boxid)
            return cell.row
        except Exception:
            return None

    def update_box_status(self, boxid: str, status: str, processed_by_tgid: int) -> bool:
        row = self.find_box_row(boxid)
        if not row:
            return False
        # columns are in header starting at some row; update absolute coordinates:
        # find header row idx to compute column numbers, but we stored columns such that:
        # A=BoxID (col 1), H = Status = col 8, I = ProcessedByTGID = col 9, J = ProcessedAt = col 10
        try:
            # ensure we have enough columns
            self.ws.update_cell(row, 8, status)
            self.ws.update_cell(row, 9, str(processed_by_tgid))
            self.ws.update_cell(row, 10, datetime.utcnow().strftime("%d-%m-%Y %H:%M:%S"))
            return True
        except Exception as e:
            logger.exception("Failed to update box status: %s", e)
            return False

    def get_boxes_by_collector(self, collector_tgid: int) -> List[List[str]]:
        hdr = self._header_row_index()
        vals = self.ws.get_all_values()[hdr:]  # rows after header (includes header as first, so slight adjust)
        out = []
        for r in vals:
            if len(r) > 3 and r[3] == str(collector_tgid):
                out.append(r)
        return out

    def get_pending_boxes(self) -> List[List[str]]:
        hdr = self._header_row_index()
        vals = self.ws.get_all_values()[hdr:]  # rows after header
        out = []
        for r in vals:
            st = r[7] if len(r) > 7 else ""
            if st in ("Новая", "В обработке"):
                out.append(r)
        return out

    def export_csv(self, path: str) -> str:
        all_vals = self.ws.get_all_values()
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerows(all_vals)
        return path

# initialize GS helper
gs = GSHelper(GOOGLE_CREDS_PATH, SHEET_KEY_OR_URL)

# ---------- Roles ----------
def get_role(user_id: int) -> str:
    if user_id in ADMIN_IDS:
        return "admin"
    if user_id in gs.get_workers():
        return "worker"
    collectors = [c[0] for c in gs.get_collectors()]
    if user_id in collectors:
        return "collector"
    return "unknown"

# ---------- Reply keyboards (ReplyKeyboardMarkup using keyboard=...) ----------
def kb_admin() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="➕ Новая коробка")],
            [KeyboardButton(text="📋 Мои коробки"), KeyboardButton(text="📦 Ожидающие")],
            [KeyboardButton(text="➕ Добавить сборщицу"), KeyboardButton(text="➕ Добавить работника")],
            [KeyboardButton(text="📤 Экспорт CSV"), KeyboardButton(text="📈 Статистика")],
            [KeyboardButton(text="🔙 В главное")]
        ],
        resize_keyboard=True
    )

def kb_worker() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📦 Ожидающие")],
            [KeyboardButton(text="📈 Статистика")],
            [KeyboardButton(text="🔙 В главное")]
        ],
        resize_keyboard=True
    )

def kb_collector() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="➕ Новая коробка")],
            [KeyboardButton(text="📋 Мои коробки")],
            [KeyboardButton(text="🔙 В главное")]
        ],
        resize_keyboard=True
    )

def kb_default() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="➕ Новая коробка")],
            [KeyboardButton(text="🔙 В главное")]
        ],
        resize_keyboard=True
    )

# small helpers keyboards used during flows
def kb_photos_ready() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="Готово")],[KeyboardButton(text="Отмена")]], resize_keyboard=True)

def kb_date_choice() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="Сегодня"), KeyboardButton(text="Ввести дату")],[KeyboardButton(text="Отмена")]], resize_keyboard=True)

def kb_destination_choice() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="WB"), KeyboardButton(text="OZON"), KeyboardButton(text="FBS")],[KeyboardButton(text="Отмена")]], resize_keyboard=True)

def kb_confirm() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="Подтвердить"), KeyboardButton(text="Отмена")]], resize_keyboard=True)

# ---------- FSM states ----------
class NewBox(StatesGroup):
    waiting_photos = State()
    waiting_collector_name = State()
    waiting_date_choice = State()
    waiting_manual_date = State()
    waiting_destination = State()
    confirming = State()

class AddCollector(StatesGroup):
    waiting_tgid = State()
    waiting_name = State()

class AddWorker(StatesGroup):
    waiting_tgid = State()

# ---------- Inline callback data ----------
class BoxActionCB(CallbackData, prefix="box"):
    action: str
    boxid: str

def worker_action_kb(boxid: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="В обработке", callback_data=BoxActionCB(action="in_process", boxid=boxid).pack()),
            InlineKeyboardButton(text="Обработана", callback_data=BoxActionCB(action="done", boxid=boxid).pack())
        ]
    ])

# ---------- Handlers ----------
@dp.message(Command("start"))
async def cmd_start(m: types.Message):
    role = get_role(m.from_user.id)
    if role == "admin":
        kb = kb_admin()
    elif role == "worker":
        kb = kb_worker()
    elif role == "collector":
        kb = kb_collector()
    else:
        kb = kb_default()
    await m.answer("Привет! Бот приёмки коробок. Кнопки внизу зависят от вашей роли.", reply_markup=kb)

# New box flow - only collectors allowed
@dp.message(F.text == "➕ Новая коробка")
async def new_box_entry(m: types.Message, state: FSMContext):
    role = get_role(m.from_user.id)
    if role != "collector" and m.from_user.id not in ADMIN_IDS:
        await m.answer("Добавлять коробки могут только зарегистрированные сборщицы.")
        return
    await state.update_data(photo_ids=[])
    await m.answer("Отправьте одно или несколько фото коробки. Когда закончите — нажмите «Готово».", reply_markup=kb_photos_ready())
    await state.set_state(NewBox.waiting_photos)

@dp.message(NewBox.waiting_photos, F.photo)
async def collect_photo(m: types.Message, state: FSMContext):
    data = await state.get_data()
    photos = data.get("photo_ids", [])
    photos.append(m.photo[-1].file_id)
    await state.update_data(photo_ids=photos)
    await m.answer(f"Фото получено ({len(photos)}). Отправьте ещё или нажмите «Готово».")

@dp.message(NewBox.waiting_photos, F.text == "Отмена")
async def cancel_newbox(m: types.Message, state: FSMContext):
    await state.clear()
    kb = kb_admin() if get_role(m.from_user.id) == "admin" else kb_collector() if get_role(m.from_user.id) == "collector" else kb_default()
    await m.answer("Процесс добавления коробки отменён.", reply_markup=kb)

@dp.message(NewBox.waiting_photos, F.text == "Готово")
async def done_photos(m: types.Message, state: FSMContext):
    data = await state.get_data()
    photos = data.get("photo_ids", [])
    if not photos:
        await m.answer("Нет фотографий. Пришлите хотя бы одно фото.")
        return
    # ask collector name (pre-fill from collectors list if possible)
    collectors = gs.get_collectors()
    name_from_meta = None
    for tgid, name in collectors:
        if tgid == m.from_user.id:
            name_from_meta = name
            break
    if name_from_meta:
        await state.update_data(collector_name=name_from_meta)
        await m.answer(f"Использую имя сборщицы: {name_from_meta}")
        await m.answer("Выберите дату коробки:", reply_markup=kb_date_choice())
        await state.set_state(NewBox.waiting_date_choice)
        return
    await m.answer("Введите имя сборщицы (или ваше имя):", reply_markup=ReplyKeyboardRemove())
    await state.set_state(NewBox.waiting_collector_name)

@dp.message(NewBox.waiting_collector_name)
async def collector_name_entered(m: types.Message, state: FSMContext):
    await state.update_data(collector_name=m.text.strip())
    await m.answer("Выберите дату коробки:", reply_markup=kb_date_choice())
    await state.set_state(NewBox.waiting_date_choice)

@dp.message(NewBox.waiting_date_choice, F.text == "Сегодня")
async def date_today_choice(m: types.Message, state: FSMContext):
    today_str = date.today().strftime("%d-%m-%Y")  # DD-MM-YYYY
    await state.update_data(box_date=today_str)
    await m.answer(f"Дата установлена: {today_str}\nВыберите назначение (WB/OZON/FBS):", reply_markup=kb_destination_choice())
    await state.set_state(NewBox.waiting_destination)

@dp.message(NewBox.waiting_date_choice, F.text == "Ввести дату")
async def date_manual_prompt(m: types.Message, state: FSMContext):
    await m.answer("Введите дату в формате DD-MM-YYYY (например 10-12-2025) или «Отмена».", reply_markup=ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="Отмена")]], resize_keyboard=True))
    await state.set_state(NewBox.waiting_manual_date)

@dp.message(NewBox.waiting_manual_date)
async def date_manual_entered(m: types.Message, state: FSMContext):
    t = m.text.strip()
    if t.lower() == "отмена":
        await state.clear()
        await m.answer("Отменено.", reply_markup=kb_collector() if get_role(m.from_user.id) == "collector" else kb_admin())
        return
    try:
        d = datetime.strptime(t, "%d-%m-%Y").date()
        await state.update_data(box_date=d.strftime("%d-%m-%Y"))
        await m.answer(f"Дата установлена: {d.strftime('%d-%m-%Y')}\nВыберите назначение:", reply_markup=kb_destination_choice())
        await state.set_state(NewBox.waiting_destination)
    except Exception:
        await m.answer("Неправильный формат. Ожидаю DD-MM-YYYY (например 10-12-2025) или напишите «Отмена».")

@dp.message(NewBox.waiting_destination)
async def destination_chosen(m: types.Message, state: FSMContext):
    txt = m.text.strip().upper()
    if txt == "ОТМЕНА":
        await state.clear()
        await m.answer("Отменено.", reply_markup=kb_collector() if get_role(m.from_user.id) == "collector" else kb_admin())
        return
    if txt not in ("WB", "OZON", "FBS"):
        await m.answer("Выберите WB, OZON или FBS.")
        return
    await state.update_data(destination=txt)
    data = await state.get_data()
    photos = data.get("photo_ids", [])
    collector_name = data.get("collector_name", "Unknown")
    box_date = data.get("box_date", date.today().strftime("%d-%m-%Y"))
    dest = data.get("destination")
    confirm_txt = (f"Подтверждение:\nСборщица: {collector_name}\nДата: {box_date}\nНазначение: {dest}\nФото: {len(photos)}\n\n"
                   "Нажмите «Подтвердить» для отправки или «Отмена»")
    await m.answer(confirm_txt, reply_markup=kb_confirm())
    await state.set_state(NewBox.confirming)

@dp.message(NewBox.confirming, F.text == "Отмена")
async def confirm_cancel(m: types.Message, state: FSMContext):
    await state.clear()
    kb = kb_collector() if get_role(m.from_user.id) == "collector" else kb_admin()
    await m.answer("Отменено.", reply_markup=kb)

@dp.message(NewBox.confirming, F.text == "Подтвердить")
async def confirm_send(m: types.Message, state: FSMContext):
    data = await state.get_data()
    photos = data.get("photo_ids", [])
    collector_name = data.get("collector_name", "Unknown")
    box_date = data.get("box_date", date.today().strftime("%d-%m-%Y"))
    destination = data.get("destination", "WB")
    collector_tgid = m.from_user.id

    boxid = gs.add_box(photos, collector_tgid, collector_name, box_date, destination)
    await m.answer(f"Коробка {boxid} добавлена. Уведомляю сотрудников склада...", reply_markup=ReplyKeyboardRemove())

    workers = gs.get_workers()
    caption = f"Новая коробка {boxid}\nСборщица: {collector_name}\nДата: {box_date}\nНазначение: {destination}\nОтправитель: {collector_tgid}"
    kb_inline = worker_action_kb(boxid)
    if not workers:
        await m.answer("В таблице нет зарегистрированных работников склада. Админ может добавить их через «➕ Добавить работника».")
    for w in workers:
        try:
            # send the first photo with inline buttons, other photos without buttons
            await bot.send_photo(w, photos[0], caption=caption, reply_markup=kb_inline)
            for fid in photos[1:]:
                await bot.send_photo(w, fid)
        except Exception as e:
            logger.exception("Notify worker failed: %s", e)

    # restore keyboard
    role = get_role(m.from_user.id)
    kb = kb_collector() if role == "collector" else kb_admin()
    await m.answer("Готово.", reply_markup=kb)
    await state.clear()

# Worker: view pending boxes
@dp.message(F.text == "📦 Ожидающие")
async def btn_pending(m: types.Message):
    role = get_role(m.from_user.id)
    if role not in ("worker", "admin"):
        await m.answer("Доступно только работникам склада или администратору.")
        return
    pending = gs.get_pending_boxes()
    if not pending:
        await m.answer("Нет ожидающих коробок.")
        return
    for r in pending:
        boxid = r[0]
        photos = r[2].split("|") if len(r) > 2 and r[2] else []
        collector_name = r[4] if len(r) > 4 else ""
        box_date = r[5] if len(r) > 5 else ""
        dest = r[6] if len(r) > 6 else ""
        status = r[7] if len(r) > 7 else ""
        caption = f"{boxid}\nСборщица: {collector_name}\nДата: {box_date}\nНазначение: {dest}\nСтатус: {status}"
        kb_inline = worker_action_kb(boxid)
        if photos:
            try:
                await bot.send_photo(m.from_user.id, photos[0], caption=caption, reply_markup=kb_inline)
                for fid in photos[1:]:
                    await bot.send_photo(m.from_user.id, fid)
            except Exception:
                await m.answer(caption)
        else:
            await m.answer(caption)

@dp.callback_query(BoxActionCB.filter())
async def worker_action_cb(cq: types.CallbackQuery, callback_data: BoxActionCB):
    role = get_role(cq.from_user.id)
    if role not in ("worker", "admin"):
        await cq.answer("У вас нет прав менять статус.", show_alert=True)
        return
    action = callback_data.action
    boxid = callback_data.boxid
    if action == "in_process":
        status = "В обработке"
    elif action == "done":
        status = "Обработана"
    else:
        await cq.answer("Неизвестное действие", show_alert=True)
        return
    ok = gs.update_box_status(boxid, status, cq.from_user.id)
    if not ok:
        await cq.answer("Не удалось обновить статус — коробка не найдена.", show_alert=True)
        return
    await cq.answer(f"Статус коробки {boxid} обновлён: {status}")
    # notify collector
    rownum = gs.find_box_row(boxid)
    if rownum:
        row = gs.ws.row_values(rownum)
        try:
            collector_tgid = int(row[3])
            await bot.send_message(collector_tgid, f"Ваша коробка {boxid} получила статус: {status} (обработал {cq.from_user.id})")
        except Exception:
            logger.info("Не удалось уведомить сборщицу.")

# Collector: my boxes
@dp.message(F.text == "📋 Мои коробки")
async def btn_my_boxes(m: types.Message):
    boxes = gs.get_boxes_by_collector(m.from_user.id)
    if not boxes:
        await m.answer("У вас нет записанных коробок.")
        return
    for r in boxes:
        boxid = r[0]
        photos = r[2].split("|") if len(r) > 2 and r[2] else []
        date_str = r[5] if len(r) > 5 else ""
        dest = r[6] if len(r) > 6 else ""
        status = r[7] if len(r) > 7 else ""
        processed_by = r[8] if len(r) > 8 else ""
        processed_at = r[9] if len(r) > 9 else ""
        txt = f"{boxid}\nДата: {date_str}\nНазначение: {dest}\nСтатус: {status}\nОбработал: {processed_by}\nВремя: {processed_at}"
        if photos:
            try:
                await bot.send_photo(m.from_user.id, photos[0], caption=txt)
                for fid in photos[1:]:
                    await bot.send_photo(m.from_user.id, fid)
            except Exception:
                await m.answer(txt)
        else:
            await m.answer(txt)

# Admin: add collector / add worker / export / stats
@dp.message(F.text == "➕ Добавить сборщицу")
async def btn_add_collector(m: types.Message, state: FSMContext):
    if m.from_user.id not in ADMIN_IDS:
        await m.answer("Только админ может добавить сборщицу.")
        return
    await m.answer("Отправьте TG ID новой сборщицы (число):", reply_markup=ReplyKeyboardRemove())
    await state.set_state(AddCollector.waiting_tgid)

@dp.message(AddCollector.waiting_tgid)
async def add_collector_tgid(m: types.Message, state: FSMContext):
    try:
        tgid = int(m.text.strip())
        await state.update_data(new_collector_tgid=tgid)
        await m.answer("Теперь введите имя (отображаемое):")
        await state.set_state(AddCollector.waiting_name)
    except Exception:
        await m.answer("Неправильный TG ID. Отправьте число.")

@dp.message(AddCollector.waiting_name)
async def add_collector_name(m: types.Message, state: FSMContext):
    data = await state.get_data()
    tgid = data.get("new_collector_tgid")
    name = m.text.strip()
    ok = gs.add_collector(tgid, name)
    if ok:
        await m.answer(f"Добавлена сборщица: {name} ({tgid})")
    else:
        await m.answer("Сборщица уже есть или произошла ошибка.")
    await state.clear()
    await m.answer("Готово.", reply_markup=kb_admin())

@dp.message(F.text == "➕ Добавить работника")
async def btn_add_worker(m: types.Message, state: FSMContext):
    if m.from_user.id not in ADMIN_IDS:
        await m.answer("Только админ может добавить работника.")
        return
    await m.answer("Отправьте TG ID работника (число):", reply_markup=ReplyKeyboardRemove())
    await state.set_state(AddWorker.waiting_tgid)

@dp.message(AddWorker.waiting_tgid)
async def add_worker_tgid(m: types.Message, state: FSMContext):
    try:
        tgid = int(m.text.strip())
        ok = gs.add_worker(tgid)
        if ok:
            await m.answer(f"Добавлен работник: {tgid}")
        else:
            await m.answer("Работник уже есть или произошла ошибка.")
    except Exception:
        await m.answer("Неправильный TG ID.")
    await state.clear()
    await m.answer("Готово.", reply_markup=kb_admin())

@dp.message(F.text == "📤 Экспорт CSV")
async def btn_export_csv(m: types.Message):
    if m.from_user.id not in ADMIN_IDS:
        await m.answer("Только админ.")
        return
    path = f"boxes_export_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}.csv"
    gs.export_csv(path)
    try:
        await m.answer_document(InputFile(path), caption="Экспорт (CSV)")
    except Exception:
        await m.answer("Ошибка отправки файла.")

@dp.message(F.text == "📈 Статистика")
async def btn_stats(m: types.Message):
    if get_role(m.from_user.id) not in ("admin", "worker"):
        await m.answer("Статистика доступна админу и работникам склада.")
        return
    # simple stats computed from sheet rows
    hdr = gs._header_row_index()
    vals = gs.ws.get_all_values()[hdr:]
    total = len(vals)
    statuses = {}
    per_collector = {}
    for r in vals:
        status = r[7] if len(r) > 7 else "?"
        statuses[status] = statuses.get(status, 0) + 1
        collector_name = r[4] if len(r) > 4 else "?"
        per_collector[collector_name] = per_collector.get(collector_name, 0) + 1
    txt = f"Всего коробок: {total}\n\nПо статусам:\n"
    for k, v in statuses.items():
        txt += f" - {k}: {v}\n"
    txt += "\nПо сборщицам:\n"
    for k, v in per_collector.items():
        txt += f" - {k}: {v}\n"
    await m.answer(txt)

@dp.message(F.text == "🔙 В главное")
async def back_to_main(m: types.Message):
    role = get_role(m.from_user.id)
    kb = kb_admin() if role=="admin" else kb_worker() if role=="worker" else kb_collector() if role=="collector" else kb_default()
    await m.answer("Возвращаемся в главное меню.", reply_markup=kb)

@dp.message()
async def fallback(m: types.Message):
    role = get_role(m.from_user.id)
    kb = kb_admin() if role=="admin" else kb_worker() if role=="worker" else kb_collector() if role=="collector" else kb_default()
    await m.answer("Непонятный ввод. Нажмите кнопку внизу.", reply_markup=kb)

# ---------- Run ----------
async def main():
    logger.info("Starting bot polling...")
    try:
        await dp.start_polling(bot)
    finally:
        await bot.session.close()

if __name__ == "__main__":
    asyncio.run(main())
