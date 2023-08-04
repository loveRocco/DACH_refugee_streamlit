import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import concurrent.futures
from tqdm import tqdm
from datetime import date, timedelta, datetime
from pygooglenews import GoogleNews
from pymongo import MongoClient


countries = {
"DE": {"de": ["Ukraine + Flüchtlinge", "Ukraine + flüchten", "Ukraine + Migranten", "Ukraine + migrieren", "Ukraine + Asyl"]},
"CH": {"de": ["Ukraine + Flüchtlinge", "Ukraine + flüchten", "Ukraine + Migranten", "Ukraine + migrieren", "Ukraine + Asyl"],
       "fr": ["Ukraine + réfugiés", "Ukraine + réfugiant", "Ukraine + migrants", "Ukraine + migrant", "Ukraine + asile"],
       "it": ["Ucraina + rifugiati", "Ucraina + rifugiato", "Ucraina + migranti", "Ucraina + migrante", "Ucraina + asilo"]},
}

delta = timedelta(days=1)

def get_content(url):
    try:
        response = requests.get(url, allow_redirects=False)

        if response.status_code in (301, 302):
            url = response.headers.get('Location')

        response = requests.get(url)
        soup = BeautifulSoup(response.content, "html.parser")
        p_tags = soup.find_all('p')
        text = "\n".join(p.get_text() for p in p_tags)
        return text if text and not text.startswith("...") else "ERROR"
    except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
        return "ERROR"

def process_search_terms(key_country, key_language, search_term, current_date, current_date_plus_one):
    try:
        gn = GoogleNews(country=key_country, lang=key_language)
        search = gn.search(search_term, from_=str(current_date), to_=str(current_date_plus_one))
        df_current = pd.DataFrame(search['entries'])
        df_current['alpha2_code'] = key_country
        df_current['language_code'] = key_language
        df_current['search_term'] = search_term
        df_current["date"] = current_date.strftime('%Y-%m-%d') 
        df_current["link"] = df_current["link"]

        with concurrent.futures.ThreadPoolExecutor(max_workers=500) as executor:
            contents = executor.map(get_content, df_current["link"])
        df_current["content"] = list(contents)

        return df_current.to_dict('records')

    except Exception as e:
        print(f"Error: {e}")
        return []

def main(delta):
    cluster = MongoClient("mongodb+srv://zhengyuan:Success2020.@cluster0.khkupyd.mongodb.net/")
    db = cluster["dbtest"]
    collection = db["google"]

    last_entry = collection.find_one(sort=[("date", -1)])
    if last_entry:
        start_date = datetime.strptime(last_entry["date"], "%Y-%m-%d").date() + timedelta(days=1)
        end_date = date.today() - timedelta(days=1)
    else:
        start_date = date(2022, 2, 21)
        end_date = date(2022, 2, 23)

    current_date = start_date

    pbar = tqdm(total=((end_date - start_date).days))

    while current_date <= end_date:
        current_date_plus_one = current_date + delta

        with concurrent.futures.ThreadPoolExecutor(max_workers=1000) as executor:
            search_terms = [(key_country, key_language, search_term, current_date, current_date_plus_one) 
                for key_country in countries 
                for key_language in countries[key_country] 
                for search_term in countries[key_country][key_language]]

            results = [executor.submit(process_search_terms, *search_term) for search_term in search_terms]

            for future in concurrent.futures.as_completed(results):
                data = future.result()
                if data:
                    collection.insert_many(data)

        pbar.update(1)
        print(f"Saved data till {current_date}")

        current_date += delta

    pbar.close()

if __name__ == '__main__':
    main(delta)