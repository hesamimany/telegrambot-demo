import sys
import os
import uuid
import asyncio
import logging
import io

from aiogram import Bot, Dispatcher, types, Router
from aiogram.filters import Command
from dotenv import load_dotenv
import boto3
from botocore.exceptions import NoCredentialsError
from botocore.client import Config

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
dp = Dispatcher()
router = Router()
dp.include_router(router)


# Upload progress callback function
def upload_progress(bytes_transferred, file_size):
    progress = (bytes_transferred / file_size) * 100
    logger.info(f"Upload progress: {progress:.2f}%")


# Start command handler
@router.message(Command("start"))
async def start_handler(message: types.Message):
    logger.info("Received /start command.")
    await message.answer("Send me a file, and I'll generate a download link for it.")


# Handler for receiving files with download and upload progress
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

    # Initialize a binary buffer to store the downloaded file data
    file_buffer = io.BytesIO()
    downloaded_bytes = 0
    chunk_size = 64 * 1024  # 64 KB per chunk

    try:
        # Get file path from Telegram
        file = await bot.get_file(file_id)
        file_path = file.file_path

        # Download the file in chunks with progress tracking
        async with bot.session.get(f'https://api.telegram.org/file/bot{BOT_TOKEN}/{file_path}') as response:
            response.raise_for_status()
            while chunk := await response.content.read(chunk_size):
                file_buffer.write(chunk)
                downloaded_bytes += len(chunk)
                download_progress = (downloaded_bytes / file_size) * 100
                logger.info(f"Download progress: {download_progress:.2f}%")

        file_buffer.seek(0)  # Rewind to the start of the buffer after download
        logger.info(f"Download completed for file '{file_name}'")

    except Exception as e:
        logger.error(f"Error downloading file: {e}")
        await message.answer(f"Error downloading file: {str(e)}")
        return

    # Upload file to Liara with progress tracking
    try:
        unique_name = f"{uuid.uuid4()}_{file_name}"
        logger.info(f"Uploading file '{file_name}' to Liara as '{unique_name}'")

        def upload_callback(bytes_transferred):
            upload_progress(bytes_transferred, file_size)

        # Upload the file with progress callback
        s3.upload_fileobj(
            file_buffer,
            LIARA_BUCKET_NAME,
            unique_name,
            Callback=upload_callback
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
