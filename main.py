import os
import logging
import time
import psutil
import traceback
import nest_asyncio
import torch
import shutil
import requests
import base64
import json
from os import walk
from datetime import datetime
from multiprocessing import Process, active_children
#shutil.rmtree('tgBotData/users_data/1275943662/.ipynb_checkpoints')
#try:
#    shutil.rmtree('tgBotData')
#except FileNotFoundError:
#    pass
# Chrome
#!sudo apt update
#!sudo apt upgrade
#!sudo apt install wget
#!wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb
#!sudo dpkg -i google-chrome-stable_current_amd64.deb
#!sudo apt-get install -f
#!sudo apt install git-all
try:
    os.remove('/content/google-chrome-stable_current_amd64.deb')
except FileNotFoundError:
    pass
# requirenments
#!pip install python-telegram-bot --upgrade
#!pip install python-telegram-bot[job-queue]
#!pip install selenium
#!pip install webdriver-manager
#!pip install undetected-chromedriver
# Additional data requered
if not 'tgBotData' in os.listdir('.') :
    #!git clone https://github.com/MatthewMikhailov/tgBotData.git
bot_token = '##############################################'
from tgBotData.parsers.wb_parser.main import run_parser_wb, return_time_wb
from tgBotData.parsers.ozon_parser.main import run_parser_ozon, return_time_ozon
from tgBotData.parsers.excel_parser.main import run_excel_parser
from webdriver_manager.chrome import ChromeDriverManager
from telegram import ForceReply, Update
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters

# Enable Google Colab compatibility
nest_asyncio.apply()

# Enable logging

logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger('apscheduler.executors.default').setLevel(logging.WARNING)
logging.getLogger().setLevel(logging.INFO)
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s", level=logging.INFO, datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)


def get_memory_usage():
    try:
        while True:
            per_cpu = psutil.cpu_percent(interval=3, percpu=True)
            for idx, usage in enumerate(per_cpu):
                if usage > 90:
                    print(f"CORE_{idx+1}: {usage}%")
            cpu_percent = psutil.cpu_percent(interval=3)
            if cpu_percent > 90:
                logger.warning(f"Program CPU Usage: {cpu_percent}%")
            memory_usage = psutil.virtual_memory().percent
            if memory_usage > 80:
                logger.warning(f"System Memory Usage: {memory_usage}%")
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info('Process CPU/RAM_use stopped')


def active_parsing_process():
    prcess_list = active_children()
    for i in prcess_list:
        if i.name == 'CPU/RAM_use':
            return len(prcess_list) - 1
    return len(prcess_list)


def get_parsing_speed():
    processes = active_parsing_process()
    if processes == 0:
        return 2
    else:
        return 1
    # elif processes == 1:
    #    return 3
    # elif processes >= 2:
    #    return 1


def load_driver():
    try:
        driverpath = ChromeDriverManager().install()
    except AttributeError:
        latest_chromedriver_version_url = "https://chromedriver.storage.googleapis.com/LATEST_RELEASE"
        latest_chromedriver_version = urllib.request.urlopen(latest_chromedriver_version_url).read().decode('utf-8')
        driverpath = ChromeDriverManager(driver_version=latest_chromedriver_version).install()
    return driverpath


async def send_file_to_excel_parsing(filename, site_name, chat_id) -> None:
    excel_pars_process = Process(target=run_excel_parser, args=(filename, site_name, chat_id),
                              name=f'{chat_id}/{filename.split("/")[-1].split(".")[0]}.xlsx/excel_parser')
    excel_pars_process.start()
    running_processes.append(excel_pars_process)


async def send_file_to_parsing_wb(filename, chat_id, max_allowed_process) -> None:
    #print('active parsing process:', len(active_children()))
    driverpath = load_driver()
    wb_pars_process = Process(target=run_parser_wb, args=(filename, chat_id, max_allowed_process, driverpath),
                              name=f'{chat_id}/{filename.split("/")[-1]}/wb_parser')
    wb_pars_process.start()
    running_processes.append(wb_pars_process)


async def send_file_to_parsing_ozon(filename, chat_id, max_allowed_process) -> None:
    #print('active parsing process:', len(active_children()))
    driverpath = load_driver()
    ozon_pars_process = Process(target=run_parser_ozon, args=(filename, chat_id, max_allowed_process, driverpath),
                                name=f'{chat_id}/{filename.split("/")[-1]}/ozon_parser')
    ozon_pars_process.start()
    running_processes.append(ozon_pars_process)


async def check_finished_jobs(context: ContextTypes.DEFAULT_TYPE) -> None:
    #print(running_processes)
    for i in running_processes:
        if not i.is_alive():
            process_data = i.name.split('/')
            context.job_queue.run_once(send_file_to_curr_id, 1, data=process_data)
            running_processes.remove(i)
    for j in send_message:
            context.job_queue.run_once(send_message_to_curr_id, 1, data=j)
            send_message.remove(j)


async def send_file_to_curr_id(context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = context.job.data[0]
    data = context.job.data[1]
    process_type = context.job.data[2]
    if 'parser' in process_type:
        filepath = f'/content/tgBotData/users_data/{chat_id}/parsed_{data}'
        await context.bot.send_document(chat_id=chat_id, document=filepath)
        os.remove(filepath)


async def send_message_to_curr_id(context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = context.job.data[0]
    data = context.job.data[1]
    await context.bot.send_message(chat_id=chat_id, text=data)


async def pars_excel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_job_counter = 0
    for job in context.job_queue.jobs():
        if str(update.message.chat_id) in job.name:
            chat_job_counter += 1
    if chat_job_counter == 1 and active_parsing_process() >= 2:
        await update.message.reply_text(
            "Бот в данный момент не может принимать более 1 запроса от пользователя")
        return None
    elif chat_job_counter > 1:
        await update.message.reply_text("Бот не может обрабатывать более 2-x запросов от пользователя одновременно")
        return None
    elif active_parsing_process() >= 4:
        await update.message.reply_text('Бот сейчас уже обрабатывает несколько запросов, попробуйте позже')
    try:
        if not update.message.text[10:].split()[0] in os.listdir(path=f"/content/tgBotData/users_data/{update.message.chat_id}"):
            await update.message.reply_text('Такого файла нет в ваших загрузках')
            await show_files(update, context)
            return None
        elif update.message.text[10:].split()[1] not in ['ВИ', 'ДМ']:
            await update.message.reply_text('Неправильно указано название сайта')
            await update.message.reply_text('Шаблон: "ВИ", "ДМ"')
            return None
    except IndexError:
        await update.message.reply_text('Шаблон вызова команды: /parsexcel <filename> <sitename>')
        return None
    filename = f"/content/tgBotData/users_data/{update.message.chat_id}/{update.message.text[10:].split()[0]}"
    site_name = update.message.text[10:].split()[1]
    await send_file_to_excel_parsing(filename, site_name, update.message.chat_id)
    finished_job_checker = True
    for job in context.job_queue.jobs():
        if 'finished_job_checker' in job.name:
            finished_job_checker = False
    if finished_job_checker:
        context.job_queue.run_repeating(check_finished_jobs, 1, first=1, last=86400, name='finished_job_checker')
    await update.message.reply_text(
        'Парсинг начался\nПо завершении процесса вам будет отправлен файл с результатом')


async def pars_ozon(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_job_counter = 0
    for job in context.job_queue.jobs():
        if str(update.message.chat_id) in job.name:
            chat_job_counter += 1
    if chat_job_counter == 1 and active_parsing_process() >= 2:
        await update.message.reply_text(
            "Бот в данный момент перегружен и не может принимать более 1 запроса от пользователя")
        return None
    elif chat_job_counter > 1:
        await update.message.reply_text("Бот не может обрабатывать более 2-x запросов от пользователя одновременно")
        return None
    elif active_parsing_process() >= 4:
        await update.message.reply_text('Бот сейчас уже обрабатывает несколько запросов, попробуйте позже')
        time = context.job_queue.jobs()[-1].name.split('_')[-2:]
        hour1 = int(time[1].split(':')[0].split()[1])
        hour2 = int(time[1].split(':')[0].split()[1])
        min1 = int(time[1].split(':')[1]) + round(int(time[0]) // 3 * 2 // 60)
        min2 = int(time[1].split(':')[1]) + round(int(time[0]) // 60)
        if min1 >= 60:
            min1 = min1 % 60
            hour1 += 1
        finish_time1 = str(hour1) + ':' + str(min1)
        if min2 >= 60:
            min2 = min2 % 60
            hour2 += 1
        finish_time2 = str(hour2) + ':' + str(min2)
        await update.message.reply_text(f'Время окончания выполнения последнего запроса:\nПримерное {finish_time1},'
                                        f' Максимальное {finish_time2}')
    try:
        if not update.message.text[10:] in os.listdir(path=f"/content/tgBotData/users_data/{update.message.chat_id}"):
            await update.message.reply_text('Такого файла нет в ваших загрузках')
            await show_files(update, context)
            return None
    except IndexError:
        await update.message.reply_text('Шаблон вызова команды: /parsozon <filename>')
        return None
    filename = f"/content/tgBotData/users_data/{update.message.chat_id}/{update.message.text.split()[1]}"
    max_allowed_process = get_parsing_speed()
    # print(max_allowed_process)
    await send_file_to_parsing_ozon(filename, update.message.chat_id, max_allowed_process)
    time = return_time_ozon(filename, max_allowed_process)
    # print(time)
    finished_job_checker = True
    for job in context.job_queue.jobs():
        if 'finished_job_checker' in job.name:
            finished_job_checker = False
    if finished_job_checker:
        context.job_queue.run_repeating(check_finished_jobs, 1, first=1, last=86400, name='finished_job_checker')
    await update.message.reply_text(
        'Парсинг начался\nПо завершении процесса вам будет отправлен файл с результатом\n'
        f'Ожидаемое время парсинга ~{round(time // 60)} мин')


async def pars_wb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_job_counter = 0
    for job in context.job_queue.jobs():
        if str(update.message.chat_id) in job.name:
            chat_job_counter += 1
    if chat_job_counter == 1 and active_parsing_process() >= 1:
        await update.message.reply_text(
            "Бот в данный момент не может принимать более 1 запроса от пользователя")
        return None
    elif chat_job_counter > 1:
        await update.message.reply_text("Бот не может обрабатывать более 2-x запросов от пользователя одновременно")
        return None
    elif active_parsing_process() >= 1:
        await update.message.reply_text('Бот сейчас уже обрабатывает несколько запросов, попробуйте позже')
    try:
        #   print(update.message.text[8:])
        if not update.message.text[8:] in os.listdir(path=f"/content/tgBotData/users_data/{update.message.chat_id}"):
            await update.message.reply_text('Такого файла нет в ваших загрузках')
            await show_files(update, context)
            return None
    except IndexError:
        await update.message.reply_text('Шаблон вызова команды: /parswb <filename>')
        return None
    filename = f"/content/tgBotData/users_data/{update.message.chat_id}/{update.message.text.split()[1]}"
    max_allowed_process = get_parsing_speed()
    # print(max_allowed_process)
    await send_file_to_parsing_wb(filename, update.message.chat_id, max_allowed_process)
    time = return_time_wb(filename, max_allowed_process)
    # print(time)
    finished_job_checker = True
    for job in context.job_queue.jobs():
        if 'finished_job_checker' in job.name:
            finished_job_checker = False
    if finished_job_checker:
        context.job_queue.run_repeating(check_finished_jobs, 1, first=1, last=86400, name='finished_job_checker')
    await update.message.reply_text(
        'Парсинг начался\nПо завершении процесса вам будет отправлен файл с результатом\n'
        f'Ожидаемое время парсинга ~{round(time // 60)} мин')


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Send a message when the command /start is issued."""
    user = update.effective_user
    await update.message.reply_html(
        rf"Hi {user.mention_html()}!",
        reply_markup=ForceReply(selective=True),
    )

async def push_users_data_to_github(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    finished_job_checker = True
    for job in context.job_queue.jobs():
        if 'finished_job_checker' in job.name:
            finished_job_checker = False
    if finished_job_checker:
        context.job_queue.run_repeating(check_finished_jobs, 1, first=1, last=86400, name='finished_job_checker')
    if update.message.chat_id == DEV_CHAT_ID:
        await update.message.reply_text('Pushing data')
        try:
            githubAPIURL = "https://api.github.com/repos/MatthewMikhailov/tgBotData/contents/"
            githubToken = "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"
            out_data = []
            f1 = {}
            file_list = []
            w1 = walk("/content/tgBotData/users_data")
            count = 1
            for (dirpath, dirnames, filenames) in w1:
                if not count == 1:
                    f1['/'.join(dirpath.split('/')[3:])] = filenames
                count += 1
            f2 = {}
            w2 = walk("/content/tgBotData/parsers")
            count = 1
            for (dirpath, dirnames, filenames) in w2:
                if not count == 1:
                    f2['/'.join(dirpath.split('/')[3:])] = filenames
                count += 1
            file_list = []
            for key in f1.keys():
                for name in f1[key]:
                  file_list.append('/'.join((key, name)))
            for key in f2.keys():
                for name in f2[key]:
                  file_list.append('/'.join((key, name)))
            for filename in file_list:
              with open(f'/content/tgBotData/{filename}', "rb") as f:
                  #print(githubAPIURL + filename)
                  getting_sha = True
                  try:
                      sha = json.loads(requests.get(githubAPIURL + filename).text)['sha']
                  except Exception as ex:
                      print(ex)
                      getting_sha = False
                  encodedData = base64.b64encode(f.read())
                  headers = {
                      "Authorization": f'''Bearer {githubToken}''',
                      "Content-type": "application/vnd.github+json"
                  }
                  data = {
                      "message": "Bot auto commit",
                      "content": encodedData.decode("utf-8"),
                  }
                  if getting_sha:
                      data['sha'] = sha
                  r = requests.put(githubAPIURL + filename, headers=headers, json=data)
                  data = json.loads(r.text)
                  try:
                      await update.message.reply_text(data['content']['name'] + '\n' + data['content']['html_url'])
                  except KeyError:
                      await update.message.reply_text(data)
        except Exception as ex:
            await update.message.reply_text(f'Error occured: {ex}')
        finally:
            await update.message.reply_text('Finished')



async def load_file(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    file = await context.bot.get_file(update.message.document)
    if ' ' in update.message.document.file_name:
        await update.message.reply_text('Название файла не должно содержать пробелы, '
                                        'переименуйте файл и повторите отправку')
        return None
    try:
        os.mkdir(f"/content/tgBotData/users_data/{str(update.message.chat_id)}")
    except FileExistsError:
        pass
    try:
        await file.download_to_drive(
            f"/content/tgBotData/users_data/{str(update.message.chat_id)}/{str(update.message.document.file_name)}")
        await update.message.reply_text('Файл сохранен')
        # print(update.message.chat_id)
    except FileExistsError:
        await update.message.reply_text('Файл с таким названием уже сохранен')


async def send_me_file(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    try:
        if not update.message.text.split()[1] in os.listdir(path=f"/content/tgBotData/users_data/{update.message.chat_id}"):
            await update.message.reply_text('Такого файла нет в ваших загрузках')
            await show_files(update, context)
            return None
    except IndexError:
        await update.message.reply_text('Шаблон вызова команды: /sendfile <filename>')
        return None
    filepath = f"/content/tgBotData/users_data/{str(update.message.chat_id)}/{update.message.text.split()[1]}"
    await context.bot.send_document(chat_id=update.message.chat_id, document=filepath)


async def del_file(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    try:
        os.remove(f'/content/tgBotData/users_data/{update.message.chat_id}/{update.message.text[1]}')
        await update.message.reply_text(f'Файл {update.message.text[1]} успешно удален')
    except Exception:
        await update.message.reply_text(f'Не удалось удалить файл {update.message.text[1]}')
        await show_files(update, context)


async def show_files(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "Список ваших файлов:\n" + '\n'.join(os.listdir(path=f"/content/tgBotData/users_data/{update.message.chat_id}")))


async def echo(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text('Я запущен, напиши /help чтобы посмотреть список комманд')


async def help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    help_data = """
        Доступные команды:
        Для загрузки файла просто отправьте его в этот чат
        /myfiles - список ваших загруженных файлов
        /delfile <filename> - удаляет загруженный файл
        /sendfile <filename> - отправляет загруженный вами файл
        /parswb <filename> - парсинг Wildberries
        /parsozon <filename> - парсинг Ozon
        /parsexcel <filename> <sitename> - УАМ в шаблон сайта
    """
    if update.message.chat_id == DEV_CHAT_ID:
        help_data = help_data + '\n/pushdatatogit - dev command'
    await update.message.reply_text(help_data)


def main() -> None:
    global running_processes, send_message, DEV_CHAT_ID
    DEV_CHAT_ID = 1275943662
    logger.info('Process CPU/RAM_use started')

    Process(target=get_memory_usage, name='CPU/RAM_use').start()
    send_message = []
    running_processes = []

    application = Application.builder().token(bot_token).concurrent_updates(True).build()

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler('help', help))
    application.add_handler(CommandHandler("myfiles", show_files))
    application.add_handler(CommandHandler('delfile', del_file))
    application.add_handler(CommandHandler('parswb', pars_wb))
    application.add_handler(CommandHandler('parsozon', pars_ozon))
    application.add_handler(CommandHandler('sendfile', send_me_file))
    application.add_handler(CommandHandler('parsexcel', pars_excel))
    application.add_handler(CommandHandler('pushdatatogit', push_users_data_to_github))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, echo))
    application.add_handler(MessageHandler(filters.ATTACHMENT, load_file))

    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    try:
        logger.info('Starting bot')
        main()
    except Exception:
        try:
            os.mkdir('error_logs')
        except FileExistsError:
            pass
        log_date = '_'.join(str(datetime.today()).split('.')[0].split(' ')).replace(':', '-')
        file_name = f'error_logs/log_{log_date}.txt'
        err_file = open(file=file_name, mode='w')
        traceback.print_exc(file=err_file)
        err_file.close()
