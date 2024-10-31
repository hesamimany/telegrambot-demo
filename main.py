import sys
import os
import uuid
import asyncio
import logging

from aiogram import Bot, Dispatcher, types, Router
from aiogram.types import ContentType
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.filters import Command
from dotenv import load_dotenv
import boto3
from botocore.exceptions import NoCredentialsError
from botocore.client import Config
from urllib.parse import quote

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

# Initialize S3 client with progress tracking
s3 = boto3.client(
    "s3",
    endpoint_url=LIARA_ENDPOINT,
    aws_access_key_id=LIARA_ACCESS_KEY,
    aws_secret_access_key=LIARA_SECRET_KEY,
    config=Config(signature_version="s3v4")
)

# Initialize Bot and Dispatcher
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())
router = Router()
dp.include_router(router)

# Progress callback function
def upload_progress(chunk, total_chunks, total_size):
    progress = (total_chunks * chunk) / total_size * 100
    logger.info(f"Upload progress: {progress:.2f}%")

# Start command handler
@router.message(Command("start"))
async def start_handler(message: types.Message):
    logger.info("Received /start command.")
    await message.answer("Send me a file, and I'll generate a download link for it.")

# Handler for receiving documents (files)
@router.message()
async def handle_document(message: types.Message):
    # Check if the message contains a document
    if not message.document:
        logger.warning("Received message without document.")
        await message.answer("Please send a file.")
        return

    file_id = message.document.file_id
    file_name = message.document.file_name
    logger.info(f"Received document with ID: {file_id} and name: {file_name}")

    # Download the file
    file = await bot.download(file_id)
    unique_name = f"{uuid.uuid4()}_{file_name}"

    try:
        # Log the start of the upload
        logger.info(f"Uploading file '{file_name}' to Liara as '{unique_name}'")

        # Upload file with progress logging
        s3.upload_fileobj(
            file,
            LIARA_BUCKET_NAME,
            unique_name,
            Callback=lambda bytes_transferred: upload_progress(
                bytes_transferred,
                file.tell(),
                message.document.file_size
            ),
        )
        logger.info(f"Upload completed for file '{unique_name}'")

    except NoCredentialsError:
        logger.error("No credentials found for Object Storage.")
        await message.answer("Error: Invalid credentials for Object Storage.")
        return
    except Exception as e:
        logger.error(f"Error uploading file: {e}")
        await message.answer(f"Error uploading file: {str(e)}")
        return

    # Generate a temporary download link (12 hours)
    pre_signed_url = s3.generate_presigned_url(
        "get_object",
        Params={"Bucket": LIARA_BUCKET_NAME, "Key": unique_name},
        ExpiresIn=12 * 60 * 60,  # 12 hours
    )
    logger.info(f"Generated pre-signed URL for '{unique_name}': {pre_signed_url}")

    await message.answer(f"File uploaded! Download it here (valid for 12 hours): {pre_signed_url}")

# Entry point
async def main():
    logger.info("Bot is starting.")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
