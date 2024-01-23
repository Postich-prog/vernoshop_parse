import requests_cache
from bs4 import BeautifulSoup
from tqdm import tqdm
import csv
import datetime as dt
from constants import BASE_DIR, DATETIME_FORMAT, MAIN_DOC_URL


if __name__ == '__main__':
    results_dir = BASE_DIR / 'results'
    results_dir.mkdir(exist_ok=True)
    now = dt.datetime.now()
    now_formatted = now.strftime(DATETIME_FORMAT)
    file_name = f'{now_formatted}.csv'
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
    a_tag_list = []
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

    page = 2
    for page in tqdm(range(2, 205), desc='Парсинг...'):
        page_url = f'https://vernoshop.com/music_catalog/rock/?page={page}'
        response = session.get(page_url)
        soup = BeautifulSoup(response.text, 'lxml')
        div_tag_list = soup.find_all('div', attrs={'class': 'caption product-info clearfix'})
        a_tag_list = []
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
