import sys
import os
import uuid
import asyncio
import logging
import json

from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv

from aiogram import Bot, Dispatcher, types, Router
from aiogram.filters import Command
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery

import boto3
from botocore.exceptions import NoCredentialsError
from botocore.client import Config
from sqlalchemy import create_engine, Column, Integer, String, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker


local_offset = timedelta(hours=3, minutes=30)
local_timezone = timezone(timedelta(hours=3, minutes=30))

# Load environment variables
load_dotenv()
LIARA_ENDPOINT = os.getenv("LIARA_ENDPOINT")
LIARA_BUCKET_NAME = os.getenv("LIARA_BUCKET_NAME")
LIARA_ACCESS_KEY = os.getenv("LIARA_ACCESS_KEY")
LIARA_SECRET_KEY = os.getenv("LIARA_SECRET_KEY")
BOT_TOKEN = os.getenv("BOT_TOKEN")

# Configure logging
logging.basicConfig(level=logging.INFO, stream=sys.stdout)
logger = logging.getLogger(__name__)

# Initialize S3 client
s3 = boto3.client(
    "s3",
    endpoint_url=LIARA_ENDPOINT,
    aws_access_key_id=LIARA_ACCESS_KEY,
    aws_secret_access_key=LIARA_SECRET_KEY,
    config=Config(signature_version="s3v4")
)

# Initialize Bot and Dispatcher
session = AiohttpSession()
bot = Bot(token=BOT_TOKEN, session=session)
dp = Dispatcher()
router = Router()
dp.include_router(router)

# Database setup
DATABASE_URL = 'sqlite:///file_links.db'
Base = declarative_base()
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)


class FileRecord(Base):
    __tablename__ = 'files'
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, index=True)
    file_name = Column(String)
    unique_name = Column(String)
    download_link = Column(String)
    expiration_time = Column(DateTime)


Base.metadata.create_all(engine)

def format_local_time(utc_datetime):
    return utc_datetime.astimezone(local_timezone).strftime('%Y-%m-%d %H:%M:%S')

# Define the inline keyboard
def main_menu_keyboard(user_id):
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Show Files", callback_data=json.dumps({"action": "show_files", "user_id": user_id}))],
        [InlineKeyboardButton(text="Show Owner (About)", callback_data=json.dumps({"action": "show_about", "user_id": user_id}))]
    ])
    return keyboard

# Update your start_handler to pass the user_id to the keyboard
@router.message(Command("start"))
async def start_handler(message: types.Message):
    user_id = message.from_user.id
    logger.info("Received /start command.")
    await message.answer("Send me a file, and I'll generate a download link for it or choose an option: ", reply_markup=main_menu_keyboard(user_id))

# Implement the /help command
@router.message(Command("help"))
async def help_handler(message: types.Message):
    await message.answer(
        "Here are the available commands:\n"
        "/files - View uploaded files\n"
        "/about - Information about this bot"
    )

# Specific handler to show about info
async def about_handler(callback_query: CallbackQuery):
    # Show owner information
    await callback_query.message.answer("This bot is created by @xX_Hes_Xx")  # Add relevant info here

# Main callback handler to route actions
@router.callback_query()
async def callback_handler(callback_query: CallbackQuery):
    data = json.loads(callback_query.data)
    action = data.get("action")
    user_id = data.get("user_id")

    if action == "show_files":
        await files_handler(callback_query, user_id)
    elif action == "show_about":
        await about_handler(callback_query)

    await callback_query.answer()  # Acknowledge the callback


async def schedule_deletion(bucket_name, file_key, delay_seconds):
    """Schedule deletion of an object and its database record after a specified delay."""
    await asyncio.sleep(delay_seconds)
    try:
        # Delete the file from S3 storage
        response = s3.delete_object(Bucket=bucket_name, Key=file_key)
        if response.get('ResponseMetadata', {}).get('HTTPStatusCode') == 204:
            logger.info(f"File {file_key} deleted successfully from storage.")

            # Delete the file record from the database
            session = Session()
            file_record = session.query(FileRecord).filter_by(unique_name=file_key).first()
            if file_record:
                session.delete(file_record)
                session.commit()
                logger.info(f"File record {file_key} deleted successfully from database.")
            else:
                logger.warning(f"File record {file_key} not found in database.")
            session.close()
        else:
            logger.warning(f"File {file_key} could not be deleted. Response: {response}")
    except Exception as e:
        logger.error(f"Error deleting file {file_key}: {e}")

# /files command handler
@router.message(Command("files"))
async def files_handler(callback_query: CallbackQuery, user_id):

    my_session = Session()
    files = my_session.query(FileRecord).filter_by(user_id=user_id).all()

    if not files:
        await callback_query.message.answer("You have no uploaded files.")
    else:
        response = "Your uploaded files:\n\n"
        for file in files:
            expiration = format_local_time(file.expiration_time)
            response += f"File: {file.file_name}\nLink: {file.download_link}\nExpires: {expiration}\n\n"
        await callback_query.message.answer(response)

    my_session.close()


# Handler for receiving files
@router.message()
async def handle_document(message: types.Message):
    file_id = None
    file_name = None
    file_size = None

    # Identify the file type and set file_id, file_name, and file_size
    if message.document:
        file_id = message.document.file_id
        file_name = message.document.file_name
        file_size = message.document.file_size
    elif message.photo:
        file_id = message.photo[-1].file_id  # Use the highest resolution photo
        file_name = f"photo_{uuid.uuid4()}.jpg"
        file_size = message.photo[-1].file_size
    elif message.audio:
        file_id = message.audio.file_id
        file_name = message.audio.file_name or f"audio_{uuid.uuid4()}.mp3"
        file_size = message.audio.file_size
    elif message.video:
        file_id = message.video.file_id
        file_name = message.video.file_name or f"video_{uuid.uuid4()}.mp4"
        file_size = message.video.file_size
    elif message.voice:
        file_id = message.voice.file_id
        file_name = f"voice_{uuid.uuid4()}.ogg"
        file_size = message.voice.file_size
    elif message.animation:
        file_id = message.animation.file_id
        file_name = message.animation.file_name or f"animation_{uuid.uuid4()}.gif"
        file_size = message.animation.file_size
    else:
        logger.warning("Unsupported file type received.")
        await message.answer("Please send a supported file type.")
        return

    logger.info(f"Received file with ID: {file_id} and name: {file_name}")

    try:
        file = await bot.get_file(file_id)
        file_buffer = await bot.download_file(file.file_path)

        # Generate unique file name and upload to Liara
        unique_name = f"{uuid.uuid4()}_{file_name}"
        s3.upload_fileobj(file_buffer, LIARA_BUCKET_NAME, unique_name)
        logger.info(f"Uploaded '{file_name}' as '{unique_name}'")

        # Generate a pre-signed URL valid for 1 hour
        pre_signed_url = s3.generate_presigned_url(
            "get_object",
            Params={"Bucket": LIARA_BUCKET_NAME, "Key": unique_name},
            ExpiresIn=3600
        )

        # Store file record in the database
        expiration_time = datetime.now(timezone.utc) + timedelta(hours=1)
        my_session = Session()
        file_record = FileRecord(
            user_id=message.from_user.id,
            file_name=file_name,
            unique_name=unique_name,
            download_link=pre_signed_url,
            expiration_time=expiration_time
        )
        my_session.add(file_record)
        my_session.commit()
        my_session.close()

        # Schedule file deletion after 1 hour
        asyncio.create_task(schedule_deletion(LIARA_BUCKET_NAME, unique_name, 3600))

        await message.answer(f"File uploaded! Download here (valid for 1 hour): {pre_signed_url}")

    except NoCredentialsError:
        logger.error("No credentials found for Object Storage.")
        await message.answer("Error: Invalid credentials for Object Storage.")
    except Exception as e:
        logger.error(f"Error uploading file: {e}")
        await message.answer(f"Error uploading file: {str(e)}")


# Entry point
async def main():
    logger.info("Bot is starting.")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
