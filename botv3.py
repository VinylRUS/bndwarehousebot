# bot.py
import os
import logging
import asyncio
import csv
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
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_IDS = [int(x.strip()) for x in os.getenv("ADMIN_ID", "").split(",") if x.strip()]
SHEET_KEY_OR_URL = os.getenv("SHEET_KEY_OR_URL")
GOOGLE_CREDS_PATH = os.getenv("GOOGLE_CREDS_PATH", "./gservice.json")

if not BOT_TOKEN or not ADMIN_IDS or not SHEET_KEY_OR_URL:
    logger.error("Missing required env vars: BOT_TOKEN, ADMIN_ID, SHEET_KEY_OR_URL")
    raise SystemExit("Please set BOT_TOKEN, ADMIN_ID, SHEET_KEY_OR_URL in .env")

bot = Bot(BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())

# ---------- Google Sheets helper ----------
class GSHelper:
    """
    Ожидаемые листы:
      - boxes (headers: BoxID,Timestamp,PhotoFileIDs,CollectorTGID,CollectorName,Date,Destination,Status,ProcessedByTGID,ProcessedAt,Notes)
      - collectors (CollectorTGID,CollectorName,AddedAt)
      - workers (WorkerTGID,AddedAt)
    """
    def __init__(self, creds_path: str, sheet_key_or_url: str):
        scope = ["https://spreadsheets.google.com/feeds","https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name(creds_path, scope)
        self.client = gspread.authorize(creds)
        self.sheet = self.client.open_by_url(sheet_key_or_url) if sheet_key_or_url.startswith("http") else self.client.open_by_key(sheet_key_or_url)
        self._ensure_worksheets()

    def _ensure_worksheets(self):
        # boxes
        try:
            self.boxes_ws = self.sheet.worksheet("boxes")
        except gspread.WorksheetNotFound:
            self.boxes_ws = self.sheet.add_worksheet("boxes", rows="2000", cols="20")
            headers = ["BoxID","Timestamp","PhotoFileIDs","CollectorTGID","CollectorName","Date","Destination","Status","ProcessedByTGID","ProcessedAt","Notes"]
            self.boxes_ws.append_row(headers)
        # collectors
        try:
            self.collectors_ws = self.sheet.worksheet("collectors")
        except gspread.WorksheetNotFound:
            self.collectors_ws = self.sheet.add_worksheet("collectors", rows="500", cols="10")
            self.collectors_ws.append_row(["CollectorTGID","CollectorName","AddedAt"])
        # workers
        try:
            self.workers_ws = self.sheet.worksheet("workers")
        except gspread.WorksheetNotFound:
            self.workers_ws = self.sheet.add_worksheet("workers", rows="200", cols="10")
            self.workers_ws.append_row(["WorkerTGID","AddedAt"])

    def _next_box_id(self) -> str:
        vals = self.boxes_ws.get_all_values()
        if len(vals) <= 1:
            return "B0001"
        last = vals[-1][0]
        try:
            num = int(last.lstrip("B")) + 1
            return f"B{num:04d}"
        except Exception:
            return f"B{len(vals):04d}"

    def add_box(self, photo_file_ids: List[str], collector_tgid: int, collector_name: str, box_date: str, destination: str, notes: str="") -> str:
        boxid = self._next_box_id()
        ts = datetime.utcnow().isoformat()
        row = [boxid, ts, "|".join(photo_file_ids), str(collector_tgid), collector_name, box_date, destination, "Новая", "", "", notes]
        self.boxes_ws.append_row(row)
        return boxid

    def find_box_row(self, boxid: str) -> Optional[int]:
        try:
            cell = self.boxes_ws.find(boxid)
            return cell.row
        except Exception:
            return None

    def update_box_status(self, boxid: str, status: str, processed_by_tgid: int) -> bool:
        row = self.find_box_row(boxid)
        if not row:
            return False
        # колонки: H=8 Status, I=9 ProcessedByTGID, J=10 ProcessedAt (1-indexed)
        self.boxes_ws.update_cell(row, 8, status)
        self.boxes_ws.update_cell(row, 9, str(processed_by_tgid))
        self.boxes_ws.update_cell(row, 10, datetime.utcnow().isoformat())
        return True

    def get_workers(self) -> List[int]:
        vals = self.workers_ws.get_all_values()[1:]
        out = []
        for r in vals:
            if r and r[0].strip():
                try:
                    out.append(int(r[0].strip()))
                except:
                    continue
        return out

    def get_collectors(self) -> List[Tuple[int,str]]:
        vals = self.collectors_ws.get_all_values()[1:]
        out = []
        for r in vals:
            if r and r[0].strip():
                try:
                    out.append((int(r[0].strip()), r[1] if len(r) > 1 else ""))
                except:
                    continue
        return out

    def add_collector(self, tgid: int, name: str):
        self.collectors_ws.append_row([str(tgid), name, datetime.utcnow().isoformat()])

    def add_worker(self, tgid: int):
        self.workers_ws.append_row([str(tgid), datetime.utcnow().isoformat()])

    def export_boxes_csv(self, path: str) -> str:
        all_vals = self.boxes_ws.get_all_values()
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerows(all_vals)
        return path

    def get_boxes_by_collector(self, collector_tgid: int) -> List[List[str]]:
        vals = self.boxes_ws.get_all_values()[1:]
        return [r for r in vals if len(r) > 3 and r[3] == str(collector_tgid)]

    def get_pending_boxes(self) -> List[List[str]]:
        vals = self.boxes_ws.get_all_values()[1:]
        return [r for r in vals if len(r) > 7 and r[7] in ("Новая","В обработке")]

    def simple_stats(self):
        vals = self.boxes_ws.get_all_values()[1:]
        total = len(vals)
        statuses = {}
        per_collector = {}
        for r in vals:
            status = r[7] if len(r) > 7 else "?"
            statuses[status] = statuses.get(status, 0) + 1
            collector = r[4] if len(r) > 4 else "?"
            per_collector[collector] = per_collector.get(collector, 0) + 1
        return {"total": total, "statuses": statuses, "per_collector": per_collector}

gs = GSHelper(GOOGLE_CREDS_PATH, SHEET_KEY_OR_URL)

# ---------- Roles ----------
def get_role(user_id: int) -> str:
    if user_id in ADMIN_IDS:
        return "admin"
    workers = gs.get_workers()
    if user_id in workers:
        return "worker"
    collectors = [c[0] for c in gs.get_collectors()]
    if user_id in collectors:
        return "collector"
    return "unknown"

# ---------- Reply keyboards (кнопки под полем ввода) ----------
def kb_admin() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton("➕ Новая коробка")],
            [KeyboardButton("📋 Мои коробки"), KeyboardButton("📦 Ожидающие")],
            [KeyboardButton("➕ Добавить сборщицу"), KeyboardButton("➕ Добавить работника")],
            [KeyboardButton("📤 Экспорт CSV"), KeyboardButton("📈 Статистика")],
            [KeyboardButton("🔙 В главное")]
        ],
        resize_keyboard=True
    )

def kb_worker() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton("📦 Ожидающие")],
            [KeyboardButton("📈 Статистика")],
            [KeyboardButton("🔙 В главное")]
        ],
        resize_keyboard=True
    )

def kb_collector() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton("➕ Новая коробка")],
            [KeyboardButton("📋 Мои коробки")],
            [KeyboardButton("🔙 В главное")]
        ],
        resize_keyboard=True
    )

def kb_default() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton("➕ Новая коробка")],
            [KeyboardButton("🔙 В главное")]
        ],
        resize_keyboard=True
    )

# Helper builders used inside handlers for temporary keyboards
def kb_photos_ready() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton("Готово")],
            [KeyboardButton("Отмена")]
        ],
        resize_keyboard=True
    )

def kb_date_choice() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton("Сегодня"), KeyboardButton("Ввести дату")],
            [KeyboardButton("Отмена")]
        ],
        resize_keyboard=True
    )

def kb_destination_choice() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton("WB"), KeyboardButton("OZON"), KeyboardButton("FBS")],
            [KeyboardButton("Отмена")]
        ],
        resize_keyboard=True
    )

def kb_confirm() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton("Подтвердить"), KeyboardButton("Отмена")]
        ],
        resize_keyboard=True
    )

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

# ---------- Inline callback for box actions ----------
class BoxActionCB(CallbackData, prefix="box"):
    action: str
    boxid: str

def worker_action_kb(boxid: str) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="В обработке", callback_data=BoxActionCB(action="in_process", boxid=boxid).pack()),
            InlineKeyboardButton(text="Обработана", callback_data=BoxActionCB(action="done", boxid=boxid).pack())
        ]
    ])
    return kb

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
    await m.answer("Привет! Я бот приёмки коробок. Кнопки под полем ввода зависят от вашей роли.", reply_markup=kb)

# New box flow
@dp.message(F.text == "➕ Новая коробка")
async def btn_newbox_pressed(m: types.Message, state: FSMContext):
    await state.update_data(photo_ids=[])
    await m.answer("Отправьте 1 или несколько фото коробки. Когда закончите — нажмите кнопку 'Готово'.", reply_markup=kb_photos_ready())
    await state.set_state(NewBox.waiting_photos)

@dp.message(NewBox.waiting_photos, F.photo)
async def collect_photo(m: types.Message, state: FSMContext):
    data = await state.get_data()
    photos = data.get("photo_ids", [])
    file_id = m.photo[-1].file_id
    photos.append(file_id)
    await state.update_data(photo_ids=photos)
    await m.answer("Фото получено. Можно отправить ещё или нажать 'Готово'.")

@dp.message(NewBox.waiting_photos, F.text == "Отмена")
async def cancel_newbox(m: types.Message, state: FSMContext):
    await state.clear()
    role = get_role(m.from_user.id)
    kb = kb_admin() if role=="admin" else kb_worker() if role=="worker" else kb_collector() if role=="collector" else kb_default()
    await m.answer("Добавление коробки отменено.", reply_markup=kb)

@dp.message(NewBox.waiting_photos, F.text == "Готово")
async def done_photos(m: types.Message, state: FSMContext):
    data = await state.get_data()
    photos = data.get("photo_ids", [])
    if not photos:
        await m.answer("Фотографий нет. Отправьте хотя бы одно фото.")
        return
    await m.answer("Введите имя сборщицы (или ваше имя):", reply_markup=ReplyKeyboardRemove())
    await state.set_state(NewBox.waiting_collector_name)

@dp.message(NewBox.waiting_photos)
async def invalid_input_waiting_photos(m: types.Message):
    await m.answer("Ожидаю фото или кнопку 'Готово'.")

@dp.message(NewBox.waiting_collector_name)
async def collector_name_entered(m: types.Message, state: FSMContext):
    name = m.text.strip()
    await state.update_data(collector_name=name)
    await m.answer("Выберите дату коробки:", reply_markup=kb_date_choice())
    await state.set_state(NewBox.waiting_date_choice)

@dp.message(NewBox.waiting_date_choice, F.text == "Сегодня")
async def date_today_cb(m: types.Message, state: FSMContext):
    today = date.today().isoformat()
    await state.update_data(box_date=today)
    await m.answer(f"Дата установлена: {today}\nВыберите назначение коробки:", reply_markup=kb_destination_choice())
    await state.set_state(NewBox.waiting_destination)

@dp.message(NewBox.waiting_date_choice, F.text == "Ввести дату")
async def date_manual_prompt(m: types.Message, state: FSMContext):
    await m.answer("Введите дату в формате YYYY-MM-DD (например 2025-12-06):", reply_markup=ReplyKeyboardMarkup(keyboard=[[KeyboardButton("Отмена")]], resize_keyboard=True))
    await state.set_state(NewBox.waiting_manual_date)

@dp.message(NewBox.waiting_manual_date)
async def date_manual_entered(m: types.Message, state: FSMContext):
    text = m.text.strip()
    if text.lower() == "отмена":
        await state.clear()
        role = get_role(m.from_user.id)
        kb = kb_admin() if role=="admin" else kb_worker() if role=="worker" else kb_collector() if role=="collector" else kb_default()
        await m.answer("Отменено.", reply_markup=kb)
        return
    try:
        d = datetime.fromisoformat(text).date()
        await state.update_data(box_date=d.isoformat())
        await m.answer(f"Дата установлена: {d.isoformat()}\nВыберите назначение коробки:", reply_markup=kb_destination_choice())
        await state.set_state(NewBox.waiting_destination)
    except Exception:
        await m.answer("Неправильный формат даты. Попробуйте YYYY-MM-DD или нажмите 'Отмена'.")

@dp.message(NewBox.waiting_destination)
async def destination_chosen(m: types.Message, state: FSMContext):
    if m.text == "Отмена":
        await state.clear()
        role = get_role(m.from_user.id)
        kb = kb_admin() if role=="admin" else kb_worker() if role=="worker" else kb_collector() if role=="collector" else kb_default()
        await m.answer("Отменено.", reply_markup=kb)
        return
    if m.text not in ("WB","OZON","FBS"):
        await m.answer("Выберите WB, OZON или FBS (или 'Отмена').")
        return
    await state.update_data(destination=m.text)
    data = await state.get_data()
    photos = data.get("photo_ids", [])
    collector_name = data.get("collector_name","Unknown")
    box_date = data.get("box_date", date.today().isoformat())
    dest = data.get("destination")
    txt = f"Подтверждение:\nСборщица: {collector_name}\nДата: {box_date}\nНазначение: {dest}\nФото: {len(photos)}\n\nНажмите 'Подтвердить' или 'Отмена'."
    await m.answer(txt, reply_markup=kb_confirm())
    await state.set_state(NewBox.confirming)

@dp.message(NewBox.confirming, F.text == "Отмена")
async def confirm_cancel(m: types.Message, state: FSMContext):
    await state.clear()
    role = get_role(m.from_user.id)
    kb = kb_admin() if role=="admin" else kb_worker() if role=="worker" else kb_collector() if role=="collector" else kb_default()
    await m.answer("Отменено.", reply_markup=kb)

@dp.message(NewBox.confirming, F.text == "Подтвердить")
async def confirm_send(m: types.Message, state: FSMContext):
    data = await state.get_data()
    photos = data.get("photo_ids", [])
    collector_name = data.get("collector_name","Unknown")
    box_date = data.get("box_date", date.today().isoformat())
    destination = data.get("destination","WB")
    collector_tgid = m.from_user.id
    boxid = gs.add_box(photos, collector_tgid, collector_name, box_date, destination)
    await m.answer(f"Коробка {boxid} добавлена в таблицу. Оповещаю работников склада...", reply_markup=ReplyKeyboardRemove())
    workers = gs.get_workers()
    caption = f"Новая коробка {boxid}\nСборщица: {collector_name}\nДата: {box_date}\nНазначение: {destination}\nОтправитель: {collector_tgid}"
    kb_inline = worker_action_kb(boxid)
    for w in workers:
        try:
            await bot.send_photo(w, photos[0], caption=caption, reply_markup=kb_inline)
            for fid in photos[1:]:
                await bot.send_photo(w, fid)
        except Exception as e:
            logger.exception(f"Failed to notify worker {w}: {e}")
    role = get_role(m.from_user.id)
    kb = kb_admin() if role=="admin" else kb_worker() if role=="worker" else kb_collector() if role=="collector" else kb_default()
    await m.answer("Готово.", reply_markup=kb)
    await state.clear()

# Worker: list pending
@dp.message(F.text == "📦 Ожидающие")
async def btn_pending(m: types.Message):
    role = get_role(m.from_user.id)
    if role not in ("worker","admin"):
        await m.answer("Доступно только работникам склада или администратору.")
        return
    pending = gs.get_pending_boxes()
    if not pending:
        await m.answer("Нет ожидающих коробок.")
        return
    for r in pending:
        boxid = r[0]
        photos = r[2].split("|") if r[2] else []
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
                await m.answer(f"{boxid} — не удалось отправить фото. Текст: {caption}", reply_markup=None)
        else:
            await m.answer(caption, reply_markup=None)

@dp.callback_query(BoxActionCB.filter())
async def worker_action_cb(cq: types.CallbackQuery, callback_data: BoxActionCB):
    role = get_role(cq.from_user.id)
    if role not in ("worker","admin"):
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
        await cq.answer("Не удалось найти коробку.", show_alert=True)
        return
    await cq.answer(f"Статус {boxid} = {status}")
    rownum = gs.find_box_row(boxid)
    if rownum:
        row = gs.boxes_ws.row_values(rownum)
        try:
            collector_tgid = int(row[3])
            await bot.send_message(collector_tgid, f"Ваша коробка {boxid} получила статус: {status} (обработал {cq.from_user.id})")
        except Exception:
            logger.info("Не удалось уведомить сборщицу.")
    else:
        logger.info("Row not found to notify collector.")

# Collector: my boxes
@dp.message(F.text == "📋 Мои коробки")
async def btn_my_boxes(m: types.Message):
    boxes = gs.get_boxes_by_collector(m.from_user.id)
    if not boxes:
        await m.answer("У вас нет записанных коробок.")
        return
    for r in boxes:
        boxid = r[0]
        photos = r[2].split("|") if r[2] else []
        date_str = r[5] if len(r) > 5 else ""
        dest = r[6] if len(r) > 6 else ""
        status = r[7] if len(r) > 7 else ""
        processed_by = r[8] if len(r) > 8 else ""
        processed_at = r[9] if len(r) > 9 else ""
        txt = f"{boxid}\nДата: {date_str}\nНазначение: {dest}\nСтатус: {status}\nОбработал: {processed_by}\nВремя обработки: {processed_at}"
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
    try:
        gs.add_collector(tgid, name)
        await m.answer(f"Добавлена сборщица: {name} ({tgid})")
    except Exception as e:
        logger.exception(e)
        await m.answer("Ошибка при добавлении в таблицу.")
    await state.clear()
    kb = kb_admin()
    await m.answer("Готово.", reply_markup=kb)

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
        gs.add_worker(tgid)
        await m.answer(f"Добавлен работник: {tgid}")
    except Exception:
        await m.answer("Неправильный TG ID.")
    await state.clear()
    kb = kb_admin()
    await m.answer("Готово.", reply_markup=kb)

@dp.message(F.text == "📤 Экспорт CSV")
async def btn_export_csv(m: types.Message):
    if m.from_user.id not in ADMIN_IDS:
        await m.answer("Только админ может экспортировать.")
        return
    path = f"boxes_export_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}.csv"
    gs.export_boxes_csv(path)
    try:
        await m.answer_document(InputFile(path), caption="Экспорт коробок (CSV)")
    except Exception:
        await m.answer("Ошибка отправки файла.")

@dp.message(F.text == "📈 Статистика")
async def btn_stats(m: types.Message):
    role = get_role(m.from_user.id)
    if role not in ("admin","worker"):
        await m.answer("Статистика доступна только админ/работникам.")
        return
    st = gs.simple_stats()
    txt = f"Всего коробок: {st['total']}\n\nПо статусам:\n"
    for k,v in st["statuses"].items():
        txt += f" - {k}: {v}\n"
    txt += "\nПо сборщицам:\n"
    for k,v in st["per_collector"].items():
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
    await m.answer("Неизвестная команда или нажата не та кнопка. Нажмите нужную кнопку внизу.", reply_markup=kb)

# ---------- Run ----------
async def main():
    try:
        logger.info("Starting polling...")
        await dp.start_polling(bot)
    finally:
        await bot.session.close()

if __name__ == "__main__":
    asyncio.run(main())
