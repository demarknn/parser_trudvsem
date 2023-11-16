import requests
import pandas as pd
import os
import logging

# Настройка logging
logging.basicConfig(filename='./ogrn.log', level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(name)s %(message)s')
logger=logging.getLogger(__name__)

LOCAL_NEW_FILE_COMPANY = './company.csv'
URL_NEW_FILE_COMPANY = 'https://opendata.trudvsem.ru/csv/company.csv'


def save_new_file_company(url):
    """скачивает новый файл company"""
    local_filename = LOCAL_NEW_FILE_COMPANY
    print('download new file')
    response = requests.get(url)
    if response.status_code == 200:
        with open(local_filename, 'wb') as file:
            file.write(response.content)
        print(f"File saved as {local_filename}")
    else:
        print(f"Failed to download the file. Status code: {response.status_code}")


def input_new_ogrn():
    """Убирает лишнее из company.csv и конвертирует его в df"""
    file_path = LOCAL_NEW_FILE_COMPANY
    try:
        df = pd.read_csv(file_path, delimiter='|')
        df.dropna(subset=['ogrn'], inplace=True)
        ogrn_values = df['ogrn'].astype('int')
        ogrn_values.columns = ['ogrn']

        output_file = 'in_all_new_file.csv'
        ogrn_values.to_csv(output_file, index=False)

        print(f'The file {file_path} with the new data has been processed successfully')
        #logger.info(f'The file {file_path}  with the new data has been processed successfully')
        return ogrn_values
    except Exception as e:
        print(f'Error processing the file {file_path} : {e}')
        logger.info(f'Error processing the file {file_path} : {e}')
        return None


def comparison_ogrn():
    """Ищет новые значения огрн, путем сравнения с уже проверенными"""
    file_path_verified = './verified_ogrn.csv'

    # Проверка наличия файла
    if not os.path.exists(file_path_verified):
        print(f'File not found: {file_path_verified}. Creating an empty file.')
        #logger.info(f'File not found: {file_path_verified}. Creating an empty file.')
        pd.DataFrame(columns=['ogrn']).to_csv(file_path_verified, index=False)

    df_verified = pd.read_csv(file_path_verified)
    df_new = input_new_ogrn()
    df_new = df_new.to_frame()

    missing_values = df_new[~df_new['ogrn'].isin(df_verified['ogrn'])]
    print(f'New values found {len(missing_values)}')
    logger.info(f'New values found {len(missing_values)}')
    return missing_values


def get_company_info(ogrn):
    """Делает запрос к апи и забирает значение из поля hiTechComplex"""
    url = f"https://trudvsem.ru/iblocks/prr_public_company_profile?companyId={ogrn}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        return data['data']['hiTechComplex']
    except requests.RequestException as e:
        print(f'Error fetching data for OGRN {ogrn}: {e}')
        #logger.info(f'Error fetching data for OGRN {ogrn}: {e}')
        return None


def save_to_csv(data_list, file_name):
    """Сохраняем в файл"""
    df = pd.DataFrame({'ogrn': data_list})
    df.to_csv(file_name, index=False, mode='a', header=False)
    print(f"Data saved to {file_name}")


def main():
    """Проходит циклом по новым записям, отправляет запрос к апи. Каждые 500 значений сохраняет результат"""
    hiTechComplex_list = []
    verified_ogrn_list = []

    list_ogrn = comparison_ogrn()
    i = 0
    for index, row in list_ogrn.iterrows():
        i += 1
        ogrn = row['ogrn']
        hiTechComplex = get_company_info(ogrn)
        verified_ogrn_list.append(ogrn)

        if hiTechComplex:
            hiTechComplex_list.append(ogrn)

        # Каждые 500 проверенных записей сохраняем результат в файл CSV, чтобы не потерять данные
        if i % 500 == 0:
            save_to_csv(verified_ogrn_list, 'verified_ogrn.csv')
            save_to_csv(hiTechComplex_list, 'hiTechComplex.csv')
            logger.info(f'checked values: {i} / found values: {len(hiTechComplex_list)}')
            hiTechComplex_list = []
            verified_ogrn_list = []

    # Сохранение остатка результатов в файл CSV
    save_to_csv(verified_ogrn_list, 'verified_ogrn.csv')
    save_to_csv(hiTechComplex_list, 'hiTechComplex.csv')
    logger.info(f'checked values: {i} / found values: {len(hiTechComplex_list)}')


if __name__ == "__main__":
    save_new_file_company(URL_NEW_FILE_COMPANY)
    main()
