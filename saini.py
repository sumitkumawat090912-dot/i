import os
import re
import time
import mmap
import datetime
import aiohttp
import aiofiles
import asyncio
import logging
import requests
import tgcrypto
import subprocess
import concurrent.futures
from math import ceil
from utils import progress_bar
from pyrogram import Client, filters
from pyrogram.types import Message
from io import BytesIO
from pathlib import Path
from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad
from base64 import b64decode


def duration(filename):
    try:
        result = subprocess.run(
            ["ffprobe", "-v", "error", "-show_entries", "format=duration",
             "-of", "default=noprint_wrappers=1:nokey=1", filename],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False
        )
        output = result.stdout.decode().strip()
        if not output:
            print(f"⚠️ Empty duration output for: {filename}")
            return 0.0
        return float(output)
    except ValueError:
        print(f"⚠️ Invalid float conversion for file: {filename}")
        return 0.0
    except Exception as e:
        print(f"⚠️ Error getting duration for {filename}: {e}")
        return 0.0


def get_mps_and_keys(api_url):
    try:
        response = requests.get(api_url, timeout=15)
        response.raise_for_status()
        response_json = response.json()
        mpd = response_json.get('MPD')
        keys = response_json.get('KEYS')
        return mpd, keys
    except Exception as e:
        print(f"⚠️ API request failed for {api_url}: {e}")
        return None, None


def exec(cmd):
    try:
        process = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        output = process.stdout.decode(errors="ignore").strip()
        print(output)
        return output
    except Exception as e:
        print(f"⚠️ Command execution error: {cmd}\nError: {e}")
        return ""


def pull_run(work, cmds):
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=work) as executor:
            print("Waiting for tasks to complete")
            fut = executor.map(exec, cmds)
            list(fut)
    except Exception as e:
        print(f"⚠️ pull_run error: {e}")


async def aio(url, name):
    k = f'{name}.pdf'
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                if resp.status == 200:
                    async with aiofiles.open(k, mode='wb') as f:
                        await f.write(await resp.read())
        return k
    except Exception as e:
        print(f"⚠️ aio download failed for {url}: {e}")
        return None


async def download(url, name):
    ka = f'{name}.pdf'
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                if resp.status == 200:
                    async with aiofiles.open(ka, mode='wb') as f:
                        await f.write(await resp.read())
        return ka
    except Exception as e:
        print(f"⚠️ async download failed for {url}: {e}")
        return None


async def pdf_download(url, file_name, chunk_size=1024 * 10):
    try:
        if os.path.exists(file_name):
            os.remove(file_name)
        r = requests.get(url, allow_redirects=True, stream=True, timeout=30)
        with open(file_name, 'wb') as fd:
            for chunk in r.iter_content(chunk_size=chunk_size):
                if chunk:
                    fd.write(chunk)
        return file_name
    except Exception as e:
        print(f"⚠️ pdf_download error for {url}: {e}")
        return None


def parse_vid_info(info):
    info = info.strip()
    info = info.split("\n")
    new_info = []
    temp = []
    for i in info:
        i = str(i)
        if "[" not in i and '---' not in i:
            while "  " in i:
                i = i.replace("  ", " ")
            i.strip()
            i = i.split("|")[0].split(" ", 2)
            try:
                if "RESOLUTION" not in i[2] and i[2] not in temp and "audio" not in i[2]:
                    temp.append(i[2])
                    new_info.append((i[0], i[2]))
            except:
                pass
    return new_info


def vid_info(info):
    info = info.strip()
    info = info.split("\n")
    new_info = dict()
    temp = []
    for i in info:
        i = str(i)
        if "[" not in i and '---' not in i:
            while "  " in i:
                i = i.replace("  ", " ")
            i.strip()
            i = i.split("|")[0].split(" ", 3)
            try:
                if "RESOLUTION" not in i[2] and i[2] not in temp and "audio" not in i[2]:
                    temp.append(i[2])
                    new_info.update({f'{i[2]}': f'{i[0]}'})
            except:
                pass
    return new_info


async def decrypt_and_merge_video(mpd_url, keys_string, output_path, output_name, quality="720"):
    try:
        output_path = Path(output_path)
        output_path.mkdir(parents=True, exist_ok=True)

        cmd1 = f'yt-dlp -f "bv[height<={quality}]+ba/b" -o "{output_path}/file.%(ext)s" --allow-unplayable-format --no-check-certificate --external-downloader aria2c "{mpd_url}"'
        print(f"Running command: {cmd1}")
        os.system(cmd1)

        avDir = list(output_path.iterdir())
        print(f"Downloaded files: {avDir}")
        print("Decrypting")

        video_decrypted = False
        audio_decrypted = False

        for data in avDir:
            try:
                if data.suffix == ".mp4" and not video_decrypted:
                    cmd2 = f'mp4decrypt {keys_string} --show-progress "{data}" "{output_path}/video.mp4"'
                    os.system(cmd2)
                    if (output_path / "video.mp4").exists():
                        video_decrypted = True
                    data.unlink()
                elif data.suffix == ".m4a" and not audio_decrypted:
                    cmd3 = f'mp4decrypt {keys_string} --show-progress "{data}" "{output_path}/audio.m4a"'
                    os.system(cmd3)
                    if (output_path / "audio.m4a").exists():
                        audio_decrypted = True
                    data.unlink()
            except Exception as e:
                print(f"⚠️ Decryption failed for {data}: {e}")

        if not video_decrypted or not audio_decrypted:
            raise FileNotFoundError("Decryption failed: video or audio file not found.")

        cmd4 = f'ffmpeg -i "{output_path}/video.mp4" -i "{output_path}/audio.m4a" -c copy "{output_path}/{output_name}.mp4"'
        os.system(cmd4)

        filename = output_path / f"{output_name}.mp4"
        if not filename.exists():
            raise FileNotFoundError("Merged video file not found.")

        return str(filename)

    except Exception as e:
        print(f"Error during decryption and merging: {str(e)}")
        return None


async def run(cmd):
    try:
        proc = await asyncio.create_subprocess_shell(
            cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE)

        stdout, stderr = await proc.communicate()

        print(f'[{cmd!r} exited with {proc.returncode}]')
        if proc.returncode == 1:
            return False
        if stdout:
            return f'[stdout]\n{stdout.decode()}'
        if stderr:
            return f'[stderr]\n{stderr.decode()}'
    except Exception as e:
        print(f"⚠️ Error running async command: {e}")
        return None


def old_download(url, file_name, chunk_size=1024 * 10):
    try:
        if os.path.exists(file_name):
            os.remove(file_name)
        r = requests.get(url, allow_redirects=True, stream=True, timeout=30)
        with open(file_name, 'wb') as fd:
            for chunk in r.iter_content(chunk_size=chunk_size):
                if chunk:
                    fd.write(chunk)
        return file_name
    except Exception as e:
        print(f"⚠️ old_download failed: {e}")
        return None


def human_readable_size(size, decimal_places=2):
    for unit in ['B', 'KB', 'MB', 'GB', 'TB', 'PB']:
        if size < 1024.0 or unit == 'PB':
            break
        size /= 1024.0
    return f"{size:.{decimal_places}f} {unit}"


def time_name():
    date = datetime.date.today()
    now = datetime.datetime.now()
    current_time = now.strftime("%H%M%S")
    return f"{date} {current_time}.mp4"


async def download_video(url, cmd, name):
    global failed_counter
    download_cmd = f'{cmd} -R 25 --fragment-retries 25 --external-downloader aria2c --downloader-args "aria2c: -x 16 -j 32"'
    print(download_cmd)
    logging.info(download_cmd)
    k = subprocess.run(download_cmd, shell=True)
    if "visionias" in cmd and k.returncode != 0 and failed_counter <= 10:
        failed_counter += 1
        await asyncio.sleep(5)
        await download_video(url, cmd, name)
    failed_counter = 0
    try:
        for ext in ["", ".webm", ".mkv", ".mp4", ".mp4.webm"]:
            test_name = name if not ext else f"{name.split('.')[0]}{ext}"
            if os.path.isfile(test_name):
                return test_name
        return name
    except Exception:
        return f"{os.path.splitext(name)[0]}.mp4"


async def send_doc(bot: Client, m: Message, cc, ka, cc1, prog, count, name, channel_id):
    reply = await bot.send_message(channel_id, f"Downloading pdf:\n<pre><code>{name}</code></pre>")
    time.sleep(1)
    start_time = time.time()
    await bot.send_document(ka, caption=cc1)
    count += 1
    await reply.delete(True)
    time.sleep(1)
    if os.path.exists(ka):
        os.remove(ka)
    time.sleep(3)


def decrypt_file(file_path, key):
    if not os.path.exists(file_path):
        return False
    try:
        with open(file_path, "r+b") as f:
            num_bytes = min(28, os.path.getsize(file_path))
            with mmap.mmap(f.fileno(), length=num_bytes, access=mmap.ACCESS_WRITE) as mmapped_file:
                for i in range(num_bytes):
                    mmapped_file[i] ^= ord(key[i]) if i < len(key) else i
        return True
    except Exception as e:
        print(f"⚠️ decrypt_file error: {e}")
        return False


async def download_and_decrypt_video(url, cmd, name, key):
    video_path = await download_video(url, cmd, name)
    if video_path:
        decrypted = decrypt_file(video_path, key)
        if decrypted:
            print(f"File {video_path} decrypted successfully.")
            return video_path
        else:
            print(f"Failed to decrypt {video_path}.")
            return None


async def send_vid(bot: Client, m: Message, cc, filename, vidwatermark, thumb, name, prog, channel_id):
    subprocess.run(f'ffmpeg -i "{filename}" -ss 00:00:10 -vframes 1 "{filename}.jpg"', shell=True)
    await prog.delete(True)
    reply1 = await bot.send_message(channel_id, f"**📩 Uploading Video 📩:-**\n<blockquote>**{name}**</blockquote>")
    reply = await m.reply_text(f"**Generate Thumbnail:**\n<blockquote>**{name}**</blockquote>")
    try:
        thumbnail = f"{filename}.jpg" if thumb == "/d" else thumb
        if vidwatermark == "/d":
            w_filename = f"{filename}"
        else:
            w_filename = f"w_{filename}"
            font_path = "vidwater.ttf"
            subprocess.run(
                f'ffmpeg -i "{filename}" -vf "drawtext=fontfile={font_path}:text=\'{vidwatermark}\':fontcolor=white@0.3:fontsize=h/6:x=(w-text_w)/2:y=(h-text_h)/2" -codec:a copy "{w_filename}"',
                shell=True
            )
    except Exception as e:
        await m.reply_text(str(e))

    dur = int(duration(w_filename))
    start_time = time.time()

    try:
        await bot.send_video(channel_id, w_filename, caption=cc, supports_streaming=True, height=720, width=1280, thumb=thumbnail, duration=dur, progress=progress_bar, progress_args=(reply, start_time))
    except Exception:
        await bot.send_document(channel_id, w_filename, caption=cc, progress=progress_bar, progress_args=(reply, start_time))

    if os.path.exists(w_filename):
        os.remove(w_filename)
    await reply.delete(True)
    await reply1.delete(True)
    if os.path.exists(f"{filename}.jpg"):
        os.remove(f"{filename}.jpg")
