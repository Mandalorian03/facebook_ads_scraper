import hashlib
import requests
import json
import time
from io import BytesIO
from urllib.parse import urlparse, parse_qs, parse_qsl
import pandas as pd
import streamlit as st
from typing import Dict, List, Any
from datetime import datetime

# Helper functions

def parse_urlencoded_to_dict(urlencoded_str: str) -> Dict[str, str]:
    return dict(parse_qsl(urlencoded_str))

def get_params_config(config_type: str, session_id: str, ad_status: str, country: str, 
                      start_date: datetime, end_date: datetime, page: str = None, 
                      query: str = None, v_value: str = '2c4a00') -> Dict[str, str]:
    common_params = {
        'session_id': session_id,
        'count': '30',
        'active_status': ad_status.upper() if ad_status != 'Both' else 'ALL',
        'ad_type': 'all',
        'country[0]': country,
        'media_type': 'all',
        'start_date': start_date.strftime('%Y-%m-%d'),
        'end_date': end_date.strftime('%Y-%m-%d'),
        'v': v_value
    }

    if config_type == 'keyword':
        if not query:
            raise ValueError("Query parameter is required for keyword type.")
        return {'q': query, 'search_type': 'keyword_exact_phrase', **common_params}
    elif config_type == 'page':
        if not page:
            raise ValueError("Page parameter is required for page type.")
        return {'view_all_page_id': page, 'search_type': 'page', **common_params}
    else:
        raise ValueError("Invalid config type. Must be 'keyword' or 'page'.")

def display_scraping_progress(item: str, status_code: int):
    if status_code == 200:
        st.success(f"Successfully scraped data for: {item} (Status Code: {status_code})")
    else:
        st.error(f"Failed to scrape data for: {item} (Status Code: {status_code})")

def get_ads_data_for_domain(params: Dict[str, str], headers: Dict[str, str], 
                            data: Dict[str, str], item: str) -> List[Dict[str, Any]]:
    results = []
    forward_cursor = ''
    collation_token = ''

    while True:
        if forward_cursor:
            params.update({'forward_cursor': forward_cursor, 'collation_token': collation_token})

        response = requests.post('https://www.facebook.com/ads/library/async/search_ads/', 
                                 headers=headers, params=params, data=data)
        
        display_scraping_progress(item, response.status_code)

        try:
            data_json = json.loads(response.text.lstrip('for (;;);'))
        except json.JSONDecodeError as e:
            st.error(f"JSONDecodeError for {item}: {e}")
            st.write(f"Response Text: {response.text[:500]}...")
            break
        
        if data_json.get('payload', {}).get('results'):
            results.extend(data_json['payload']['results'])
        else:
            break

        forward_cursor = data_json['payload'].get('forwardCursor')
        collation_token = data_json['payload'].get('collationToken')
        if not forward_cursor:
            break

        time.sleep(1)

    return results

def extract_from_url(url: str, key: str) -> str:
    parsed_url = urlparse(url)
    query_params = parse_qs(parsed_url.query)
    return query_params.get(key, [''])[0]

def extract_domain(url: str) -> str:
    parsed_url = urlparse(url)
    return parsed_url.netloc

def ensure_serializable(data: Any) -> Any:
    if isinstance(data, bytes):
        return data.decode('utf-8')
    elif isinstance(data, dict):
        return {k: ensure_serializable(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [ensure_serializable(item) for item in data]
    return data

def process_ads_data(ads_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    extracted_data = []
    for item in ads_data:
        if isinstance(item, list):
            for sub_item in item:
                extracted_data.extend(process_ads_data([sub_item]))
            continue
        
        snapshot = item.get('snapshot', {})
        cards = snapshot.get('cards', [{}])

        if 'link_url' in snapshot:
            link_url = snapshot.get('link_url', '')
            original_image_url = snapshot.get('images', [{}])[0].get('original_image_url', '') if snapshot.get('images') else ''
            original_video_url = snapshot.get('videos', [{}])[0].get('video_hd_url', '') if snapshot.get('videos') else ''
            body = snapshot.get('body', {}).get('markup', {}).get('__html', '')
            cta_text = snapshot.get('cta_text', '')
            title = snapshot.get('title', '')
            link_description = snapshot.get('link_description', '')
        else:
            card = cards[0] if cards else {}
            link_url = card.get('link_url', '')
            original_image_url = card.get('original_image_url', '')
            original_video_url = card.get('video_hd_url', '')
            body = card.get('body', '')
            cta_text = card.get('cta_text', '')
            title = card.get('title', '')
            link_description = card.get('link_description', '')

        creation_time_raw = snapshot.get('creation_time')
        end_date_raw = item.get('endDate')

        creation_time = pd.to_datetime(creation_time_raw, unit='s', errors='coerce').strftime('%Y-%m-%d') if creation_time_raw else None
        end_date = pd.to_datetime(end_date_raw, unit='s', errors='coerce').strftime('%Y-%m-%d') if end_date_raw else None

        extracted_item = {
            'adid': item.get('adid', ''),
            'pageid': item.get('pageID', ''),
            'pagename': item.get('pageName', ''),
            'link_url': link_url,
            'body': body,
            'cta_text': cta_text,
            'title': title,
            'original_image_url': original_image_url,
            'original_video_url': original_video_url,
            'caption': snapshot.get('caption', ''),
            'creation_time': creation_time,
            'end_date': end_date,
            'collationCount': item.get('collationCount', 0),
            'display_format': snapshot.get('display_format', ''),
            'link_description': link_description,
            'domain': extract_domain(link_url),
            'keywords': extract_from_url(link_url, 'sqs'),
            'atxt': extract_from_url(link_url, 'atxt')
        }
        extracted_data.append(ensure_serializable(extracted_item))

    return extracted_data

def save_to_excel(data: List[Dict[str, Any]], filename: str = 'ad_details_sorted_by_collation_count.xlsx') -> str:
    ads_details = []
    for ad in data:
        creation_time = ad.get('creation_time')
        end_date = ad.get('end_date')

        days_running = None
        if creation_time and end_date:
            try:
                days_running = (pd.to_datetime(end_date) - pd.to_datetime(creation_time)).days
            except ValueError:
                pass

        ads_details.append({
            'Page Name': ad.get('pagename', ''),
            'Link URL': ad.get('link_url', ''),
            'Title': ad.get('title', ''),
            'Original Image/Video URL': ad.get('original_image_url') or ad.get('original_video_url', ''),
            'No of Days Running': days_running,
            'Collation Count': ad.get('collationCount', 0),
            'Creation Time': creation_time,
            'End Date': end_date
        })

    ads_details_df = pd.DataFrame(ads_details)
    ads_details_df_sorted = ads_details_df.sort_values(by='Collation Count', ascending=False)
    ads_details_df_sorted.to_excel(filename, index=False)

    return filename

def main():
    st.title('Facebook Ads Scraper')

    st.sidebar.header('Session Configuration')
    session_id = st.sidebar.text_input('Session ID', '')
    cookie_value = st.sidebar.text_input('Cookie Value', '')
    v_value = st.sidebar.text_input('v Value', '2c4a00')

    data_format = st.sidebar.selectbox('Data Dictionary Format', ['JSON', 'URL-encoded'])
    data_input = st.sidebar.text_area('Data Dictionary', '', height=200)

    data_dict = {}
    if data_input:
        try:
            data_dict = json.loads(data_input) if data_format == 'JSON' else parse_urlencoded_to_dict(data_input)
        except json.JSONDecodeError:
            st.sidebar.error('Invalid JSON format')

    st.sidebar.header('Scraping Configuration')
    scraping_mode = st.sidebar.selectbox('Scraping Mode', ['Keywords', 'Page IDs'])
    input_data = st.sidebar.text_area(f'Enter {scraping_mode} (comma separated)', '', height=100)

    start_date = st.sidebar.date_input('Start Date')
    end_date = st.sidebar.date_input('End Date')
    ad_status = st.sidebar.selectbox('Ad Status', ['Active', 'Paused', 'Both'])
    country = st.sidebar.selectbox('Country', ['US', 'CA', 'UK', 'AU', 'ALL'])

    if st.sidebar.button('Start Scraping'):
        headers = {
            'accept': '*/*',
            'accept-language': 'en-US,en;q=0.9,sq;q=0.8',
            'cache-control': 'no-cache',
            'content-type': 'application/x-www-form-urlencoded',
            'cookie': cookie_value,
            'origin': 'https://www.facebook.com',
            'pragma': 'no-cache',
            'referer': 'https://www.facebook.com/ads/library/',
            'sec-ch-prefers-color-scheme': 'dark',
            'sec-ch-ua': '"Google Chrome";v="123", "Not:A-Brand";v="8", "Chromium";v="123"',
            'sec-ch-ua-full-version-list': '"Google Chrome";v="123.0.6312.107", "Not:A-Brand";v="8.0.0.0", "Chromium";v="123.0.6312.107"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-model': '""',
            'sec-ch-ua-platform': '"macOS"',
            'sec-ch-ua-platform-version': '"14.4.0"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
            'x-asbd-id': '129477',
            'x-fb-lsd': 'h0OO4QSgNGaiz79WvnPyEf'
        }

        ads_data = []
        items_to_scrape = [item.strip() for item in input_data.split(',') if item.strip()]
        
        progress_bar = st.progress(0)
        for i, item in enumerate(items_to_scrape):
            st.write(f"Scraping data for: {item}")
            params = get_params_config(
                'keyword' if scraping_mode == 'Keywords' else 'page',
                session_id, ad_status, country, start_date, end_date,
                query=item if scraping_mode == 'Keywords' else None,
                page=item if scraping_mode == 'Page IDs' else None,
                v_value=v_value
            )
            ads_data.extend(get_ads_data_for_domain(params, headers, data_dict, item))
            progress_bar.progress((i + 1) / len(items_to_scrape))

        extracted_data = process_ads_data(ads_data)
        
        st.write('Processed Data')
        df = pd.DataFrame(extracted_data)
        st.dataframe(df)

        filename = save_to_excel(extracted_data)
        st.success('Scraping Completed!')
        st.download_button(
            label="Download Excel file",
            data=open(filename, "rb").read(),
            file_name=filename,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

if __name__ == "__main__":
    main()