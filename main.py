from aiogram import Bot, Dispatcher, types, Router
from aiogram.types import ContentType
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.filters import Command
from dotenv import load_dotenv
import os
import uuid
import boto3
from botocore.exceptions import NoCredentialsError
from urllib.parse import quote
import asyncio

# Load environment variables
load_dotenv()
LIARA_ENDPOINT = os.getenv("LIARA_ENDPOINT")
LIARA_BUCKET_NAME = os.getenv("LIARA_BUCKET_NAME")
LIARA_ACCESS_KEY = os.getenv("LIARA_ACCESS_KEY")
LIARA_SECRET_KEY = os.getenv("LIARA_SECRET_KEY")

# Initialize S3 client
s3 = boto3.client(
    "s3",
    endpoint_url=LIARA_ENDPOINT,
    aws_access_key_id=LIARA_ACCESS_KEY,
    aws_secret_access_key=LIARA_SECRET_KEY,
)

# Telegram bot token
BOT_TOKEN = os.getenv("BOT_TOKEN")

# Initialize Bot and Dispatcher
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())
router = Router()
dp.include_router(router)

# Start command handler
@router.message(Command("start"))
async def start_handler(message: types.Message):
    await message.answer("Send me a file, and I'll generate a download link for it.")

# Handler for receiving documents (files)
@router.message()
async def handle_document(message: types.Message):
    # Check if the message contains a document
    if not message.document:
        await message.answer("Please send a file.")
        return

    file_id = message.document.file_id
    file = await bot.download(file_id)
    unique_name = f"{uuid.uuid4()}_{message.document.file_name}"

    # Upload file to Liara Object Storage
    try:
        s3.upload_fileobj(file, LIARA_BUCKET_NAME, unique_name)
    except NoCredentialsError:
        await message.answer("Error: Invalid credentials for Object Storage.")
        return
    except Exception as e:
        await message.answer(f"Error uploading file: {str(e)}")
        return

    # Generate a temporary download link (12 hours)
    pre_signed_url = s3.generate_presigned_url(
        "get_object",
        Params={"Bucket": LIARA_BUCKET_NAME, "Key": unique_name},
        ExpiresIn=12 * 60 * 60,  # 12 hours
    )

    await message.answer(f"File uploaded! Download it here (valid for 12 hours): {pre_signed_url}")

# Entry point
async def main():
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
