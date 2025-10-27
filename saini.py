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

# ensure a global failed_counter exists (used in download_video)
try:
    failed_counter
except NameError:
    failed_counter = 0


def duration(filename):
    """
    Returns duration as float. Safe decode and fallback to 0.0 on error.
    Keeps same behavior as original but prevents float(b'') crash.
    """
    try:
        result = subprocess.run(
            ["ffprobe", "-v", "error", "-show_entries", "format=duration", "-of",
             "default=noprint_wrappers=1:nokey=1", filename],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            check=False
        )
        output = result.stdout.decode(errors="ignore").strip()
        if not output:
            # ffprobe returned nothing
            print(f"⚠️ Empty duration output for: {filename}")
            return 0.0
        try:
            return float(output)
        except Exception:
            print(f"⚠️ Could not convert duration output to float: {repr(output)}")
            return 0.0
    except Exception as e:
        print(f"⚠️ Error in duration(): {e}")
        return 0.0


def get_mps_and_keys(api_url):
    """
    Original logic preserved: fetch MPD and KEYS from API response keys 'MPD' and 'KEYS'.
    Added safe checks for empty/invalid response.
    """
    try:
        response = requests.get(api_url, timeout=15)
        if not response.text or not response.text.strip():
            print("⚠️ Empty API response in get_mps_and_keys()")
            return None, None
        try:
            response_json = response.json()
        except Exception as e:
            print(f"⚠️ JSON parse error in get_mps_and_keys(): {e}")
            return None, None
        mpd = response_json.get('MPD')
        keys = response_json.get('KEYS')
        return mpd, keys
    except Exception as e:
        print(f"⚠️ Request failed in get_mps_and_keys(): {e}")
        return None, None


def get_mps_and_keys2(api_url):
    """
    Variant that expects 'mpd_url' and 'keys' fields.
    """
    try:
        response = requests.get(api_url, timeout=15)
        if not response.text or not response.text.strip():
            print("⚠️ Empty API response in get_mps_and_keys2()")
            return None, None
        try:
            response_json = response.json()
        except Exception as e:
            print(f"⚠️ JSON parse error in get_mps_and_keys2(): {e}")
            return None, None
        mpd = response_json.get('mpd_url')
        keys = response_json.get('keys')
        return mpd, keys
    except Exception as e:
        print(f"⚠️ Request failed in get_mps_and_keys2(): {e}")
        return None, None


def get_mps_and_keys3(api_url):
    """
    Variant that returns single 'url' field.
    """
    try:
        response = requests.get(api_url, timeout=15)
        if not response.text or not response.text.strip():
            print("⚠️ Empty API response in get_mps_and_keys3()")
            return None
        try:
            response_json = response.json()
        except Exception as e:
            print(f"⚠️ JSON parse error in get_mps_and_keys3(): {e}")
            return None
        mpd = response_json.get('url')
        return mpd
    except Exception as e:
        print(f"⚠️ Request failed in get_mps_and_keys3(): {e}")
        return None


def exec(cmd):
    """
    Run a command and return stdout (decoded). Preserves original print behavior.
    """
    try:
        process = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        output = process.stdout.decode(errors="ignore")
        print(output)
        return output
    except Exception as e:
        print(f"⚠️ exec() error for cmd {cmd}: {e}")
        return ""


def pull_run(work, cmds):
    """
    Run commands concurrently using ThreadPoolExecutor.
    Preserves original API (max_workers=work) and prints waiting message.
    """
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=work) as executor:
            print("Waiting for tasks to complete")
            fut = executor.map(exec, cmds)
            # consume generator so tasks actually run before function returns
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
                else:
                    print(f"⚠️ aio() HTTP status {resp.status} for {url}")
                    return None
    except Exception as e:
        print(f"⚠️ aio() error for {url}: {e}")
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
                else:
                    print(f"⚠️ download() HTTP status {resp.status} for {url}")
                    return None
    except Exception as e:
        print(f"⚠️ download() error: {e}")
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
            i = i.strip()
            i = i.split("|")[0].split(" ", 2)
            try:
                if "RESOLUTION" not in i[2] and i[2] not in temp and "audio" not in i[2]:
                    temp.append(i[2])
                    new_info.append((i[0], i[2]))
            except Exception:
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
            i = i.strip()
            i = i.split("|")[0].split(" ", 3)
            try:
                if "RESOLUTION" not in i[2] and i[2] not in temp and "audio" not in i[2]:
                    temp.append(i[2])
                    new_info.update({f'{i[2]}': f'{i[0]}'})
            except Exception:
                pass
    return new_info


async def decrypt_and_merge_video(mpd_url, keys_string, output_path, output_name, quality="720"):
    """
    Preserve original flow:
    - yt-dlp download (kept original -f selection)
    - mp4decrypt to decrypt files
    - ffmpeg to merge
    Added small safeguards and -N 4 to reduce concurrency (helps Heroku memory).
    """
    try:
        output_path = Path(output_path)
        output_path.mkdir(parents=True, exist_ok=True)

        # Keep format selection same but add -N 4 (safer concurrency)
        cmd1 = f'yt-dlp -N 4 -f "bv[height<={quality}]+ba/b" -o "{output_path}/file.%(ext)s" --allow-unplayable-format --no-check-certificate --external-downloader aria2c "{mpd_url}"'
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
                    print(f"Running command: {cmd2}")
                    os.system(cmd2)
                    if (output_path / "video.mp4").exists():
                        video_decrypted = True
                    # remove original chunk
                    try:
                        data.unlink()
                    except Exception:
                        pass
                elif data.suffix == ".m4a" and not audio_decrypted:
                    cmd3 = f'mp4decrypt {keys_string} --show-progress "{data}" "{output_path}/audio.m4a"'
                    print(f"Running command: {cmd3}")
                    os.system(cmd3)
                    if (output_path / "audio.m4a").exists():
                        audio_decrypted = True
                    try:
                        data.unlink()
                    except Exception:
                        pass
            except Exception as e:
                print(f"⚠️ Decryption failed for {data}: {e}")

        if not video_decrypted or not audio_decrypted:
            raise FileNotFoundError("Decryption failed: video or audio file not found.")

        cmd4 = f'ffmpeg -i "{output_path}/video.mp4" -i "{output_path}/audio.m4a" -c copy "{output_path}/{output_name}.mp4"'
        print(f"Running command: {cmd4}")
        os.system(cmd4)
        # cleanup intermediate files if present
        try:
            if (output_path / "video.mp4").exists():
                (output_path / "video.mp4").unlink()
        except Exception:
            pass
        try:
            if (output_path / "audio.m4a").exists():
                (output_path / "audio.m4a").unlink()
        except Exception:
            pass

        filename = output_path / f"{output_name}.mp4"
        if not filename.exists():
            raise FileNotFoundError("Merged video file not found.")

        # Optional: print duration info as original did
        try:
            cmd5 = f'ffmpeg -i "{filename}" 2>&1 | grep "Duration"'
            duration_info = os.popen(cmd5).read()
            print(f"Duration info: {duration_info}")
        except Exception:
            pass

        return str(filename)

    except Exception as e:
        print(f"Error during decryption and merging: {str(e)}")
        raise


async def run(cmd):
    """
    Async run wrapper; preserve original outputs.
    """
    try:
        proc = await asyncio.create_subprocess_shell(
            cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout, stderr = await proc.communicate()
        print(f'[{cmd!r} exited with {proc.returncode}]')
        if proc.returncode == 1:
            return False
        if stdout:
            return f'[stdout]\n{stdout.decode()}'
        if stderr:
            return f'[stderr]\n{stderr.decode()}'
        return None
    except Exception as e:
        print(f"⚠️ Error running async command: {e}")
        return None


def old_download(url, file_name, chunk_size=1024 * 10):
    """
    Synchronous download similar to original, with timeout.
    """
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
    """
    Preserve original flow but safe-guard failed_counter and file checks.
    """
    global failed_counter
    try:
        download_cmd = f'{cmd} -R 25 --fragment-retries 25 --external-downloader aria2c --downloader-args "aria2c: -x 16 -j 32"'
        print(download_cmd)
        logging.info(download_cmd)
        k = subprocess.run(download_cmd, shell=True)
        if "visionias" in cmd and k.returncode != 0 and failed_counter <= 10:
            failed_counter += 1
            await asyncio.sleep(5)
            return await download_video(url, cmd, name)
        failed_counter = 0
        try:
            # check variants as original
            if os.path.isfile(name):
                return name
            elif os.path.isfile(f"{name}.webm"):
                return f"{name}.webm"
            base = name.split(".")[0]
            if os.path.isfile(f"{base}.mkv"):
                return f"{base}.mkv"
            elif os.path.isfile(f"{base}.mp4"):
                return f"{base}.mp4"
            elif os.path.isfile(f"{base}.mp4.webm"):
                return f"{base}.mp4.webm"
            return name
        except FileNotFoundError:
            return os.path.splitext(name)[0] + ".mp4"
    except Exception as e:
        print(f"⚠️ download_video error: {e}")
        return name


async def send_doc(bot: Client, m: Message, cc, ka, cc1, prog, count, name, channel_id):
    reply = await bot.send_message(channel_id, f"Downloading pdf:\n<pre><code>{name}</code></pre>")
    time.sleep(1)
    start_time = time.time()
    try:
        await bot.send_document(ka, caption=cc1)
    except Exception as e:
        print(f"⚠️ send_doc send error: {e}")
    count += 1
    try:
        await reply.delete(True)
    except Exception:
        pass
    time.sleep(1)
    if ka and os.path.exists(ka):
        try:
            os.remove(ka)
        except Exception:
            pass
    time.sleep(3)


def decrypt_file(file_path, key):
    """
    Keep original XOR-like decrypt logic; add safe guards.
    """
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
    return None


async def send_vid(bot: Client, m: Message, cc, filename, vidwatermark, thumb, name, prog, channel_id):
    """
    Preserve original behavior: generate thumbnail, optionally watermark, send video;
    fixed the broken 'progress' argument insertion and added safe file checks.
    """
    try:
        subprocess.run(f'ffmpeg -i "{filename}" -ss 00:00:10 -vframes 1 "{filename}.jpg"', shell=True)
    except Exception as e:
        print(f"⚠️ Thumbnail generation failed: {e}")

    try:
        await prog.delete(True)
    except Exception:
        pass

    reply1 = await bot.send_message(channel_id, f"**📩 Uploading Video 📩:-**\n<blockquote>**{name}**</blockquote>")
    reply = await m.reply_text(f"**Generate Thumbnail:**\n<blockquote>**{name}**</blockquote>")
    try:
        if thumb == "/d":
            thumbnail = f"{filename}.jpg"
        else:
            thumbnail = thumb

        if vidwatermark == "/d":
            w_filename = f"{filename}"
        else:
            w_filename = f"w_{filename}"
            font_path = "vidwater.ttf"
            try:
                subprocess.run(
                    f'ffmpeg -i "{filename}" -vf "drawtext=fontfile={font_path}:text=\'{vidwatermark}\':fontcolor=white@0.3:fontsize=h/6:x=(w-text_w)/2:y=(h-text_h)/2" -codec:a copy "{w_filename}"',
                    shell=True
                )
            except Exception as e:
                print(f"⚠️ Watermarking failed: {e}")

    except Exception as e:
        await m.reply_text(str(e))
        # If generation failed, continue to attempt send with original filename
        thumbnail = f"{filename}.jpg" if os.path.exists(f"{filename}.jpg") else None
        w_filename = filename

    # safe duration conversion (duration() already safe)
    dur = int(duration(w_filename)) if w_filename else 0
    start_time = time.time()

    try:
        # try to send as video; if fails, fallback to document (preserve original behavior)
        await bot.send_video(channel_id, w_filename, caption=cc, supports_streaming=True,
                             height=720, width=1280, thumb=thumbnail, duration=dur,
                             progress=progress_bar, progress_args=(reply, start_time))
    except Exception:
        try:
            await bot.send_document(channel_id, w_filename, caption=cc,
                                    progress=progress_bar, progress_args=(reply, start_time))
        except Exception as e:
            print(f"⚠️ send_vid both send_video and send_document failed: {e}")

    # cleanup: remove files if they exist (preserve original removal order)
    try:
        if os.path.exists(w_filename):
            os.remove(w_filename)
    except Exception:
        pass

    try:
        await reply.delete(True)
    except Exception:
        pass

    try:
        await reply1.delete(True)
    except Exception:
        pass

    try:
        if os.path.exists(f"{filename}.jpg"):
            os.remove(f"{filename}.jpg")
    except Exception:
        pass
