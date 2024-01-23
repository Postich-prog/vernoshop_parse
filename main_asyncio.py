import requests_cache
from bs4 import BeautifulSoup
from tqdm import tqdm
import csv
import datetime as dt
from constants import BASE_DIR, DATETIME_FORMAT, MAIN_DOC_URL
import asyncio
import aiohttp


songs_data = []


async def get_page_data(session, page):
    url = f'https://vernoshop.com/music_catalog/rock/?page={page}'

    async with session.get(url=url) as response:
        response_text = await response.text()
        soup = BeautifulSoup(response_text, features='lxml')
        div_tag_list = soup.find_all('div', attrs={'class': 'caption product-info clearfix'})
        for div in div_tag_list:
            a_tag = div.find('a', attrs={'data-hpm-href': '1'})
            link = a_tag['href']
            response = await session.get(link)
            response_text = await response.text()
            soup = BeautifulSoup(response_text, features='lxml')
            div_left = soup.find_all('div', attrs={'class': 'dotted-line_left'})

            song = []

            name = ''
            album = ''
            code = ''
            origin = ''
            label = ''
            number = ''
            price = ''
            for div in div_left:
                if div.text == 'Производитель:':
                    if name != '':
                        label = div.find_next('div', attrs={'class': 'dotted-line_right'})
                        label = label.text
                    else:
                        name = div.find_next('div', attrs={'class': 'dotted-line_right'})
                        name = name.text
                elif div.text == 'Альбом:':
                    album = div.find_next('div', attrs={'class': 'dotted-line_right'})
                    album = album.text
                elif div.text == 'Штрих-код:':
                    code = div.find_next('div', attrs={'class': 'dotted-line_right'})
                    code = code.text + '\n'
                elif div.text == 'Происхождение:':
                    origin = div.find_next('div', attrs={'class': 'dotted-line_right'})
                    origin = origin.text
                elif div.text == 'Страна:':
                    origin = div.find_next('div', attrs={'class': 'dotted-line_right'})
                    origin = origin.text
                elif div.text == 'Носителей:':
                    number = div.find_next('div', attrs={'class': 'dotted-line_right'})
                    number = number.text

            price = soup.find('span', attrs={'class': 'update_price'})
            text_price = ''
            if price:
                price = price.text.split(' ')
                for digit in price:
                    if digit.isnumeric():
                        text_price += digit

            song.append(name)
            song.append(album)
            song.append(code)
            song.append(origin)
            song.append(label)
            song.append(number)
            song.append(text_price)
            songs_data.append(song)
            print(name+'\n')


async def gather_data():
    async with aiohttp.ClientSession() as session:
        tasks = []

        for page in range(2, 205):
            task = asyncio.create_task(get_page_data(session, page))
            tasks.append(task)

        await asyncio.gather(*tasks)


def main():
    asyncio.run(gather_data())

    results_dir = BASE_DIR / 'results'
    results_dir.mkdir(exist_ok=True)
    now = dt.datetime.now()
    now_formatted = now.strftime(DATETIME_FORMAT)
    file_name = f'{now_formatted}_async.csv'
    file_path = results_dir / file_name
    with open(file_path, 'w', encoding='utf-8-sig') as f:
        writer = csv.writer(f, delimiter=';', dialect='unix')
        writer.writerow([
            'Наименование (Производитель)',
            'Альбом', 'Штрих-код', 'Страна производитель (происхождение)',
            'Лейбл', 'Штук в комплекте (носителей)',
            'Цена'
        ])

    session = requests_cache.CachedSession()
    response = session.get(MAIN_DOC_URL)
    soup = BeautifulSoup(response.text, features='lxml')
    div_tag_list = soup.find_all('div', attrs={'class': 'caption product-info clearfix'})
    for div in div_tag_list:
        a_tag = div.find('a', attrs={'data-hpm-href': '1'})
        link = a_tag['href']
        response = session.get(link)
        soup = BeautifulSoup(response.text, features='lxml')
        div_left = soup.find_all('div', attrs={'class': 'dotted-line_left'})

        name = ''
        album = ''
        code = ''
        origin = ''
        label = ''
        number = ''
        price = ''
        for div in div_left:
            if div.text == 'Производитель:':
                if name != '':
                    label = div.find_next('div', attrs={'class': 'dotted-line_right'})
                    label = label.text
                else:
                    name = div.find_next('div', attrs={'class': 'dotted-line_right'})
                    name = name.text
            elif div.text == 'Альбом:':
                album = div.find_next('div', attrs={'class': 'dotted-line_right'})
                album = album.text
            elif div.text == 'Штрих-код:':
                code = div.find_next('div', attrs={'class': 'dotted-line_right'})
                code = code.text + '\n'
            elif div.text == 'Происхождение:':
                origin = div.find_next('div', attrs={'class': 'dotted-line_right'})
                origin = origin.text
            elif div.text == 'Страна:':
                origin = div.find_next('div', attrs={'class': 'dotted-line_right'})
                origin = origin.text
            elif div.text == 'Носителей:':
                number = div.find_next('div', attrs={'class': 'dotted-line_right'})
                number = number.text

        price = soup.find('span', attrs={'class': 'update_price'})
        if price:
            price = price.text.split(' ')
            text_price = ''

            for digit in price:
                if digit.isnumeric():
                    text_price += digit
        with open(file_path, 'a', encoding='utf-8-sig') as f:
            # Создаём «объект записи» writer.
            writer = csv.writer(f, delimiter=';', dialect='unix')
            # Передаём в метод writerows список с результатами парсинга.
            writer.writerow([
                name, album, code,
                origin, label, number,
                text_price
            ])

    for row in tqdm(songs_data, desc='Загрузка данных в файл'):
        with open(file_path, 'a', encoding='utf-8-sig') as f:
            # Создаём «объект записи» writer.
            writer = csv.writer(f, delimiter=';', dialect='unix')
            # Передаём в метод writerows список с результатами парсинга.
            writer.writerow(row)


if __name__ == '__main__':
    main()
