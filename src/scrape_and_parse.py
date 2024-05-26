import os
import ezodf
import re
import traceback
import requests
import time
import glob
from bs4 import BeautifulSoup
import ntpath
import sys
import re
from unidecode import unidecode
import pandas as pd
import logging
import xlrd
module_logger = logging.getLogger('centgovspend_application')
base = 'https://www.gov.uk/government/'
pubs = base + 'publications/'
data = 'https://data.gov.uk/'


def read_date(date):
    return xlrd.xldate.xldate_as_datetime(date, 0)


#def read_ods(filename, sheet_no=0, header=0):
#    tab = ezodf.opendoc(filename=filename).sheets[sheet_no]
#    df = pd.DataFrame({col[header].value: [x.value for x in col[header + 1:]]
#                       for col in tab.columns()})
#    df = df.T.reset_index(drop=False).T
#    df = df.drop(columns=[list(df)[-1]], axis=1)
#    return df.T.reset_index(drop=False).T


from odf.opendocument import load
from odf.table import Table, TableRow, TableCell
from odf.text import P


def read_ods(file_path, sheet_index=0):
    """
    Reads an .ods file and returns a pandas DataFrame.

    Parameters:
    - file_path: str, path to the .ods file.
    - sheet_index: int, index of the sheet to read (default is 0, the first sheet).

    Returns:
    - pandas.DataFrame: DataFrame containing the data from the .ods file.
    """
    # Load the .ods file
    doc = load(file_path)

    # Get all the sheets
    sheets = doc.spreadsheet.getElementsByType(Table)

    # Check if the sheet_index is within the range of available sheets
    if sheet_index >= len(sheets):
        raise IndexError(f"Sheet index {sheet_index} is out of range. The document has {len(sheets)} sheets.")

    # Select the desired sheet
    sheet = sheets[sheet_index]

    data = []
    for row in sheet.getElementsByType(TableRow):
        row_data = []
        for cell in row.getElementsByType(TableCell):
            # Each cell can have multiple paragraphs (P elements)
            cell_values = [p.firstChild.data if p.firstChild else '' for p in cell.getElementsByType(P)]
            cell_value = ''.join(cell_values)
            row_data.append(cell_value)
        data.append(row_data)

    # Create DataFrame from the data
    df = pd.DataFrame(data)

    return df

def merge_files(rawpath):
    frame = pd.DataFrame()
    list_ = []
    for file_ in glob.glob(os.path.join(rawpath, '..', 'output',
                                        'mergeddepts', '*.csv')):
        df = pd.read_csv(file_, index_col=None, low_memory=False,
                         header=0, encoding='latin-1',
                         dtype={'transactionnumber': str,
                                'amount': float,
                                'supplier': str,
                                'date': str,
                                'expensearea': str,
                                'expensetype': str,
                                'file': str})

        df['dept'] = ntpath.basename(file_)[:-4]
        list_.append(df)
    frame = pd.concat(list_, sort=False)
    frame.dropna(thresh=0.90 * len(df), axis=1, inplace=True)
    if pd.to_numeric(frame['date'], errors='coerce').notnull().all():
        frame['date'] = pd.to_datetime(frame['date'].apply(read_date),
                                       format='%Y-%m-%d',#T%H:%M:%S',
#                                       dayfirst=True,
                                       errors='coerce')
    else:
        df['date'] = pd.to_datetime(df['date'],
                                    format='%Y-%m-%d',#T%H:%M:%S',
#                                    dayfirst=True,
                                    errors='coerce')
    frame['transactionnumber'] = frame['transactionnumber'].str.replace('[^\w\s]', '')
    frame['transactionnumber'] = frame['transactionnumber'].str.strip("0")
    return frame


def get_data(datalocations, filepath, department, exclusions=[]):
    ''' send data.gov.uk or gov.uk data through here. '''
    for datalocation in datalocations:
        r = requests.get(datalocation)
        listcsvs = []
        listxls = []
        listxlsx = []
        listods = []
        soup = BeautifulSoup(r.content, 'lxml')
        for link in soup.findAll('a'):
            if link.get('href').lower().endswith('.csv'):
                if 'data.gov.uk' in datalocation:
                    listcsvs.append(link.get('href'))
                else:
                    listcsvs.append('https://www.gov.uk/' + link.get('href'))
            elif link.get('href').lower().endswith('.xlsx'):
                if 'data.gov.uk' in datalocation:
                    listxlsx.append(link.get('href'))
                else:
                    listxlsx.append('https://www.gov.uk/' + link.get('href'))
            elif link.get('href').lower().endswith('.xls'):
                if 'data.gov.uk' in datalocation:
                    listxls.append(link.get('href'))
                else:
                    listxls.append('https://www.gov.uk/' + link.get('href'))
            elif link.get('href').lower().endswith('.ods'):
                if 'data.gov.uk' in datalocation:
                    listods.append(link.get('href'))
                else:
                    listods.append('https://www.gov.uk/' + link.get('href'))
        if len([listcsvs, listxls, listxlsx, listods]) > 0:
            for filelocation in set(sum([listcsvs, listxls,
                                         listxlsx, listods], [])):
                if 'https://assets' in filelocation:
                    filelocation = filelocation.replace(
                        'https://www.gov.uk/', '')
                try:
                    breakout = 0
                    for exclusion in exclusions:
                        if exclusion in str(filelocation):
                            module_logger.info(
                                os.path.basename(filelocation) + ' is excluded! Verified problem.')
                            breakout = 1
                    if breakout == 1:
                        continue
                except Exception as e:
                    pass
                filename = os.path.basename(
                    filelocation).replace('?', '').lower()
                while filename[0].isalpha() is False:
                    filename = filename[1:]
                if ('gpc' not in filename.lower()) \
                        and ('procurement' not in filename.lower()) \
                        and ('card' not in filename.lower()):
                    if os.path.exists(os.path.join(filepath, department,
                                                   filename)) is False:
                        try:
                            r = requests.get(filelocation)
                            module_logger.info('File downloaded: ' +
                                               ntpath.basename(filelocation))
                            with open(os.path.join(
                                      os.path.join(filepath, department),
                                      filename), "wb") as csvfile:
                                csvfile.write(r.content)
                        except Exception as e:
                            module_logger.debug('Problem downloading ' +
                                                ntpath.basename(filelocation) +
                                                ': ' + str(e))
                        time.sleep(1.5)


def heading_replacer(columnlist, filepath):
    columnlist = [x if str(x) != 'nan' else 'dropme' for x in columnlist]
    columnlist = ['dropme' if str(x) == '-1.0' else x for x in columnlist]
    columnlist = [x if str(x) != '£' else 'amount' for x in columnlist]
    columnlist = [unidecode(x) if type(x) is str else x for x in columnlist]
    if ('Total Amount' in columnlist) and ('Amount' in columnlist):
        columnlist = ['dropme' if x ==
                      'Total Amount' else x for x in columnlist]
    if ('Gross' in columnlist) and ('Nett Amount' in columnlist):
        columnlist = ['Amount' if str(x) == 'Gross' else x for x in columnlist]
        columnlist = ['dropme' if str(x) ==
                      'Nett Amount' else x for x in columnlist]
    if ('Gross' in columnlist) and ('NET ' in columnlist):
        columnlist = ['Amount' if str(x) == 'Gross' else x for x in columnlist]
        columnlist = ['dropme' if str(x) ==
                      'NET ' else x for x in columnlist]
    if ('Gross' in columnlist) and ('Amount' not in columnlist):
        columnlist = ['Amount' if str(x) ==
                      'Gross' else x for x in columnlist]
    if ('Mix of Nett & Gross' in columnlist) and ('Amount' not in columnlist):
        columnlist = ['Amount' if str(x) ==
                      'Mix of Nett & Gross' else x for x in columnlist]

    columnlist = [
        'dropme' if 'departmentfamily' in str(x) else x for x in columnlist]
    columnlist = ['amount' if str(x) == '£' else x for x in columnlist]
    columnlist = [''.join(filter(str.isalpha, str(x).lower()))
                  for x in columnlist]
    replacedict = pd.read_csv(os.path.join(
        filepath, '..', '..', 'support', 'replacedict.csv'),
        header=None, dtype={0: str}).set_index(0).squeeze().to_dict()
    for item in range(len(columnlist)):
        for key, value in replacedict.items():
            if key == columnlist[item]:
                columnlist[item] = columnlist[item].replace(key, value)
    return columnlist


def parse_data(filepath, department, filestoskip=[]):
    allFiles = glob.glob(os.path.join(filepath, department, '*'))
    frame = pd.DataFrame()
    list_ = []
    filenames = []
    removefields = pd.read_csv(os.path.join(
        filepath, '..', '..', 'support', 'remfields.csv'),
        names=['replacement'])['replacement'].values.tolist()
    for file_ in allFiles:
        if ntpath.basename(file_).split('.')[0] not in filenames:
            filenames.append(ntpath.basename(file_).split('.')[0])
            try:
                if ntpath.basename(file_) in [x.lower() for x in filestoskip]:
                    module_logger.info(ntpath.basename(file_) +
                                       ' is excluded! Verified problem.')
                    continue
                if os.path.getsize(file_) == 0:
                    module_logger.info(ntpath.basename(
                        file_) + ' is 0b: skipping')
                    continue
                if file_.lower().endswith(tuple(['.csv', '.xls', '.xlsx', '.ods'])) is False:
                    module_logger.debug(ntpath.basename(file_) +
                                        ': not csv, xls, xlsx or ods: not parsing....')
                    continue
                try:
                    if (file_.lower().endswith('.xlsx')):
                        df = pd.read_excel(file_, index_col=None,# encoding='latin-1',
                                           header=None,# on_bad_lines='skip',
                                           engine = 'openpyxl'#,
                                           #skip_blank_lines=True
                                           #warn_bad_lines=False
                        )
                    elif (file_.endswith('xls')):
                        df = pd.read_excel(file_, index_col=None,# encoding='latin-1',
                                           header=None,# on_bad_lines='skip',
                                           engine = 'pyxlsb'#,
                                           #skip_blank_lines=True
                                           #warn_bad_lines=False
                        )
                    elif (file_.lower().endswith('.csv')):
                        df = pd.read_csv(file_, index_col=None, encoding='latin-1',
                                         header=None, on_bad_lines='skip',
                                         skip_blank_lines=True,
                                         #warn_bad_lines=False,
                                         engine='python')
                    elif (file_.lower().endswith('.ods')):
                        df = read_ods(file_)
                    if ntpath.basename(file_).lower() == 'dcms_transactions_over__25k_january_2016__1_.csv':
                        df.loc[-1] = ['Department family', 'Entity', 'Date',
                                      'Expense Type', 'Expense area', 'Supplier',
                                      'Transation number', 'Amount', 'Narrative']
                        df.index = df.index + 1  # shifting index
                        df = df.sort_index()
                    if len(df.columns) < 3:
                        if df.iloc[0].str.contains('!DOC').any():
                            module_logger.debug(ntpath.basename(
                                file_) + ': html. Delete.')
                        elif df.iloc[0].str.contains('no data', case=False).any():
                            module_logger.debug(ntpath.basename(file_)
                                                + ' has no data in it.')
                        else:
                            module_logger.debug(ntpath.basename(
                                file_) + ': not otherwise tab. ')
                        continue
                    if ntpath.basename(file_) == 'september_2013_publishable_spend_over__25k_csv.csv':
                        df.loc[0, 5] = 'supplier'
                    while (((any("supplier" in str(s).lower() for s in list(df.iloc[0]))) is False)
                           and ((any("merchant" in str(s).lower() for s in list(df.iloc[0]))) is False)
                           and ((any("payee" in str(s).lower() for s in list(df.iloc[0]))) is False)
                           and ((any("merchant name" in str(s).lower() for s in list(df.iloc[0]))) is False)
                           and ((any("supplier name" in str(s).lower() for s in list(df.iloc[0]))) is False)) \
                        or (((any("amount" in str(s).lower() for s in list(df.iloc[0]))) is False)
                            and ((any("total" in str(s).lower() for s in list(df.iloc[0]))) is False)
                            and ((any("gross" in str(s).lower() for s in list(df.iloc[0]))) is False)
                            and ((any("£" in str(s).lower() for s in list(df.iloc[0]))) is False)
                            and ((any("spend" in str(s).lower() for s in list(df.iloc[0]))) is False)
                            #                    and ((any("sum of amount" in str(s).lower() for s in list(df.iloc[0]))) is False)
                            and ((any("mix of nett & gross" in str(s).lower() for s in list(df.iloc[0]))) is False)
                            and ((any("value" in str(s).lower() for s in list(df.iloc[0]))) is False)):
                        try:
                            df = df.iloc[1:]
                        except Exception as e:
                            module_logger.debug('Problem with trimming' +
                                                ntpath.basename(file_) +
                                                '. ' + str(e))
                    df.columns = heading_replacer(list(df.iloc[0]), filepath)
                    if len(df.columns.tolist()) != len(set(df.columns.tolist())):
                        df = df.loc[:, ~df.columns.duplicated()]
                    df = df.iloc[1:]
                    df.rename(columns=lambda x: x.strip(), inplace=True)
                    # drop empty rows and columns where half the cells are empty
                    df.dropna(thresh=4, axis=0, inplace=True)
                    df.dropna(thresh=0.75 * len(df), axis=1, inplace=True)
                    df['file'] = ntpath.basename(file_)
                    if department == 'dfeducation': #cut exec agencies here
                        if 'idepartmentfamily' in df.columns:
                            df.rename(columns={'idepartmentfamily': 'departmentfamily'}, inplace=True)
                        try:
                            df = df[(df['entity'].str.upper() == 'DEPARTMENT FOR EDUCATION') |
                                    (df['departmentfamily'].str.upper() == 'DEPARTMENT FOR EDUCATION')]
                        except Exception as e:
                            print('Whats going on here?' + e)
                    if list(df).count('amount') == 0 and list(df).count('gross') == 1:
                        df = df.rename(columns={'gross': 'amount'})
                    if list(df).count('amount') == 0 and list(df).count('grossvalue') == 1:
                        df = df.rename(columns={'grossvalue': 'amount'})
                    if len(df) > 0:
                        try:
                            df['amount'] = df['amount'].astype(str).str.replace(
                                ',', '').str.extract('(\d+)',
                                                     expand=False).astype(float)
                        except Exception as e:
                            module_logger.debug("Can't convert amount to float in " +
                                                ntpath.basename(file_) + '. ' +
                                                'Columns in this file ' +
                                                df.columns.tolist())
                        if df.empty is False:
                            list_.append(df)
                    else:
                        module_logger.info('No data in ' +
                                           ntpath.basename(file_) + '!')
                except Exception as e:
                    module_logger.debug('Problem with ' + ntpath.basename(file_) +
                                        ': ' + traceback.format_exc())
                    try:
                        module_logger.debug('The columns are: ' +
                                            str(df.columns.tolist()))
                    except ValueError:
                        pass
                    try:
                        module_logger.debug('The first row: ' + str(df.iloc[0]))
                    except ValueError:
                        pass
            except Exception as e:
                module_logger.debug('Something undetermined wrong with' +
                                    file_ + '. Heres the traceback: ' +
                                    str(e))
    frame = pd.concat(list_, sort=False)
    for column in frame.columns.tolist():
        if column.lower() in removefields:
            frame.drop([column], inplace=True, axis=1)
        if (column == ' ') or (column == ''):
            frame.drop([column], inplace=True, axis=1)
    #    frame = frame.drop_duplicates(keep='first', inplace=True)
    if 'nan' in list(frame):
        frame = frame.drop(labels=['nan'], axis=1)
    return frame


def createdir(filepath, dept):
    ''' check if the necessary subdirectory, and if not, make it'''
    if os.path.exists(os.path.join(filepath, dept)) is False:
        os.makedirs(os.path.join(filepath, dept))
    print('Working on ' + dept + '.')
    module_logger.info('Working on ' + dept + '.')


def dfeducation(filepath, dept):
    ''' Notes: collections with annual groupings, fairly clean.
    Notes: nb -- there is no page for 2018-2019 at present.
    How to update: look for early 2018 files in the collection page:
    /collections/dfe-department-and-executive-agency-spend-over-25-000
    Most recent file: oct 2018
    '''
    createdir(filepath, dept)
    if 'noscrape' not in sys.argv:
        base1 = 'department-for-education-and-executive-agency-spend-over-25000'
        base2 = 'department-for-education'
        dataloc = [pubs + 'dfe-and-executive-agency-spend-over-25000-2023-to-2024',
                   pubs + 'dfe-and-executive-agency-spend-over-25000-2022-to-2023',
                   pubs + 'dfe-and-executive-agency-spend-over-25000-2021-to-2022',
                   pubs + 'dfe-and-executive-agency-spend-over-25000-2020-to-2021',
                   pubs + 'dfe-and-executive-agency-spend-over-25000-2019-to-2020',
                   pubs + 'dfe-and-executive-agency-spend-over-25000-2018-to-2019',
                   pubs + 'dfe-and-executive-agency-spend-over-25000-2017-to-2018',
                   pubs + 'dfe-and-executive-agency-spend-over-25000-2016-to-2017',
                   pubs + 'dfe-and-executive-agency-spend-over-25000-2015-to-2016',
                   pubs + 'dfe-and-executive-agency-spend-over-25000-2014-to-2015',
                   pubs + 'department-for-education-and-executive-agency-spend-over-25000-financial-year-2013-to-2014',
                   pubs + '201213-department-for-education-and-executive-agency-spend-over-25000',
                   pubs + 'departmental-and-alb-spend-over-25000-in-201112',
                   pubs + 'department-for-education-and-alb-spend-over-25000-201011'
                   ]
        get_data(dataloc, filepath, dept)
    try:
        df = parse_data(filepath, dept,
                        filestoskip = ['dfe_spend_01apr_2012.csv',
                                       'dfe_spend_03jun_2012.csv'])
        df.to_csv(os.path.join(filepath, '..', '..', 'output',
                               'mergeddepts', dept + '.csv'), index=False)
    except Exception as e:
        module_logger.debug('CRITICAL problem: Cannot construct a merged '
                            'output dataframe for ' + dept)


def dohealth(filepath, dept):
    ''' Notes: collections with annual groupings, very logical annual subdomains
    Notes: However, department changes from DH to DHSC in end 2017 reshuffle.
    How to update: /collections/spending-over-25-000--2
    Most recent file: Dec 2018
    '''
    createdir(filepath, dept)
    if 'noscrape' not in sys.argv:
        dataloc = [pubs + 'dhsc-spending-over-25000-february-2024',
                   pubs + 'dhsc-spending-over-25000-january-2024',
                   pubs + 'dhsc-spending-over-25000-december-2023',
                   pubs + 'dhsc-spending-over-25000-november-2023',
                   pubs + 'dhsc-spending-over-25000-october-2023',
                   pubs + 'dhsc-spending-over-25000-september-2023',
                   pubs + 'dhsc-spending-over-25000-august-2023',
                   pubs + 'dhsc-spending-over-25000-july-2023',
                   pubs + 'dhsc-spending-over-25000-june-2023',

                   pubs + 'dhsc-spending-over-25000-april-2023',
                   pubs + 'dhsc-spending-over-25000-march-2023',
                   pubs + 'dhsc-spending-over-25000-february-2023',
                   pubs + 'dhsc-spending-over-25000-january-2023',
                   pubs + 'dhsc-spending-over-25000-december-2022',
                   pubs + 'dhsc-spending-over-25000-november-2022',
                   pubs + 'dhsc-spending-over-25000-september-2022',
                   pubs + 'dhsc-spending-over-25000-august-2022',
                   pubs + 'dhsc-spending-over-25000-january-2022',

                   pubs + 'dhsc-spending-over-25000-december-2021',
                   pubs + 'dhsc-spending-over-25000-november-2021',
                   pubs + 'dhsc-spending-over-25000-october-2021',
                   pubs + 'dhsc-spending-over-25000-september-2021',
                   pubs + 'dhsc-spending-over-25000-august-2021',
                   pubs + 'dhsc-spending-over-25000-july-2021',
                   pubs + 'dhsc-spending-over-25000-june-2021',
                   pubs + 'dhsc-spending-over-25000-february-2021',
                   pubs + 'dhsc-spending-over-25000-january-2021',
                   pubs + 'dhsc-spending-over-25000-december-2020',
                   pubs + 'dhsc-spending-over-25000-november-2020',
                   pubs + 'dhsc-spending-over-25000-october-2020',

                   pubs + 'dhsc-spending-over-25000-september-2020',
                   pubs + 'dhsc-spending-over-25000-august-2020',
                   pubs + 'dhsc-spending-over-25000-july-2020',
                   pubs + 'dhsc-spending-over-25000-september-2021',
                   pubs + 'dhsc-spending-over-25000-august-2021',
                   pubs + 'dhsc-spending-over-25000-july-2021',
                   pubs + 'dhsc-spending-over-25000-june-2020',
                   pubs + 'dhsc-spending-over-25000-may-2020',
                   pubs + 'dhsc-spending-over-25000-april-2020',
                   pubs + 'dhsc-spending-over-25000-march-2020',
                   pubs + 'dhsc-spending-over-25000-february-2020',
                   pubs + 'dhsc-spending-over-25000-january-2020',

                   pubs + 'dhsc-spending-over-25000-december-2019',
                   pubs + 'dhsc-spending-over-25000-november-2019',
                   pubs + 'dhsc-spending-over-25000-october-2019',
                   pubs + 'dhsc-spending-over-25000-september-2019',
                   pubs + 'dhsc-spending-over-25000-august-2019',
                   pubs + 'dhsc-spending-over-25000-july-2019',
                   pubs + 'dhsc-spending-over-25000-june-2019',
                   pubs + 'dhsc-spending-over-25000-may-2019',
                   pubs + 'dhsc-spending-over-25000-april-2019',
                   pubs + 'dhsc-spending-over-25000-march-2019',
                   pubs + 'dhsc-spending-over-25000-february-2019',
                   pubs + 'dhsc-spending-over-25000-january-2019',

                   pubs + 'dh-departmental-spend-over-25-000-2010',
                   pubs + 'dh-departmental-spend-over-25-000-2011',
                   pubs + 'dh-departmental-spend-over-25-000-2012',
                   pubs + 'dh-departmental-spend-over-25-000-2013',
                   pubs + 'dh-departmental-spend-over-25-000-2014',
                   pubs + 'dh-departmental-spend-over-25-000-2015',
                   pubs + 'dh-departmental-spend-over-25-000-2016',
                   pubs + 'dh-departmental-spend-over-25-000-2017',
                   pubs + 'dh-departmental-spend-over-25-000-2018',
                   ]
        get_data(dataloc, filepath, dept)
    try:
        df = parse_data(filepath, dept,
                        filestoskip=['december_18_over__25k_spend_data_to_be_published.csv'])
        df.to_csv(os.path.join(filepath, '..', '..', 'output',
                               'mergeddepts', dept + '.csv'), index=False)
    except Exception as e:
        module_logger.debug('CRITICAL problem: Cannot construct a merged '
                            'output dataframe for ' + dept)


def dftransport(filepath, dept):
    ''' Notes: everything in one data.gov.uk page, easy to scrape
    How to update: check dataset/financial-transactions-data-dft
    Most recent file: may 2017
    '''
    createdir(filepath, dept)
    if 'noscrape' not in sys.argv:
        dataloc = [data + 'dataset/financial-transactions-data-dft']
        get_data(dataloc, filepath, dept)
    try:
        df = parse_data(filepath, dept,
                        filestoskip=['dft-monthly-transparency-data-dec-2016.xlsx',
                                     'dft-monthly-transparency-data-feb-2017.xlsx',
                                     'dft-monthly-transparency-data-jan-2017.xlsx',
                                     'dft-monthly-spend-201112.xls',
                                     'dft-monthly-spend-201201.xls',
                                     'dft-spending-over-25000-august-23.csv',
                                     'dft-monthly-transparency-data-nov-2016.xlsx',
                                     'dft-monthly-transparency-data-mar-2017.xlsx',
                                     'dft-monthly-transparency-data-apr-2017.xlsx',
                                     'dft-monthly-transparency-data-may-2017.csv',
                                     'dft-spending-over-25000-april-2023.csv',
                                     'dft-monthly-spend-201409.csv',
                                     'dft-spending-over-25000-july-23.csv',
                                     'dft-monthly-transparency-data-september-2018.csv'])
        df.to_csv(os.path.join(filepath, '..', '..', 'output',
                               'mergeddepts', dept + '.csv'), index=False)
    except Exception as e:
        module_logger.debug('CRITICAL problem: Cannot construct a merged '
                            'output dataframe for ' + dept)


def cabinetoffice(filepath, dept):
    ''' Notes: everything in one publications page
    How to update: publications/cabinet-office-spend-data
    Most recent file: dec 2018
    '''
    createdir(filepath, dept)
    if 'noscrape' not in sys.argv:
        dataloc = [pubs + 'cabinet-office-spend-data']
        get_data(dataloc, filepath, dept)
    try:
        df = parse_data(filepath, dept)
        df.to_csv(os.path.join(filepath, '..', '..', 'output',
                               'mergeddepts', dept + '.csv'), index=False)
    except Exception as e:
        module_logger.debug('CRITICAL problem: Cannot construct a merged '
                            'output dataframe for ' + dept)


def dfintdev(filepath, dept):
    ''' Notes: everything in one data.gov.uk page, easy. Threshold £500
    How to update: check dataset/financial-transactions-data-dft
    Most recent file: Jan 2019
    '''
    createdir(filepath, dept)
    if 'noscrape' not in sys.argv:
        dataloc = [data + 'dataset/8446e151-5123-47c7-9570-3b960c144104/spend-transactions-by-dfid']
        get_data(dataloc, filepath, dept)
    try:
        df = parse_data(filepath, dept, filestoskip=['feb2015.csv',
                                                     'January2014.csv',
                                                     'may-2016.csv'])
        df.to_csv(os.path.join(filepath, '..', '..', 'output',
                               'mergeddepts', dept + '.csv'), index=False)
    except Exception as e:
        module_logger.debug('CRITICAL problem: Cannot construct a merged '
                            'output dataframe for ' + dept)


def dfinttrade(filepath, dept):
    ''' Notes: Dept only created in 2016. Groupled publications in collection
        for the most part of the first two years, then buggy/bad individual
        files hyperlinked to the collection page.
        How to update: check /collections/dit-departmental-spending-over-25000
        Most recent file: Dec 2018
    '''
    createdir(filepath, dept)
    if 'noscrape' not in sys.argv:
        dataloc = [pubs + 'dit-spending-over-25000-january-2021',
                   pubs + 'dit-spending-over-25000-for-2022',
                   pubs + 'dit-spending-over-25000-december-2020',
                   pubs + 'dit-spending-over-25000-november-2020',
                   pubs + 'dit-spending-over-25000-october-2020',
                   pubs + 'dit-spending-over-25000-september-2020',
                   pubs + 'dit-spending-over-25000-august-2020',
                   pubs + 'dit-spending-over-25000-july-2020',
                   pubs + 'dit-spending-over-25000-june-2020',
                   pubs + 'dit-spending-over-25000-may-2020',
                   pubs + 'dit-spending-over-25000-april-2020',
                   pubs + 'dit-spending-over-25000-march-2020',
                   pubs + 'dit-spending-over-25000-february-2020',
                   pubs + 'dit-spending-over-25000-january-2020',

                   pubs + 'dit-spending-over-25000-december-2019',
                   pubs + 'dit-spending-over-25000-november-2019',
                   pubs + 'dit-spending-over-25000-october-2019',
                   pubs + 'dit-spending-over-25000-september-2019',
                   pubs + 'dit-spending-over-25000-august-2019',
                   pubs + 'dit-spending-over-25000-july-2019',
                   pubs + 'dit-spending-over-25000-june-2019',
                   pubs + 'dit-spending-over-25000-may-2019',
                   pubs + 'dit-spending-over-25000-april-2019',
                   pubs + 'dit-spending-over-25000-march-2019',
                   pubs + 'dit-spending-over-25000-february-2019',
                   pubs + 'dit-spending-over-25000-january-2019',

                   pubs + 'dit-spending-over-25000-december-2018',
                   pubs + 'dit-spending-over-25000-november-2018',
                   pubs + 'dit-spending-over-25000-october-2018',
                   pubs + 'dit-spending-over-25000-september-2018',
                   pubs + 'dit-spending-over-25000-august-2018',
                   pubs + 'dit-spending-over-25000-july-2018',
                   pubs + 'dit-spending-over-25000-june-2018',
                   pubs + 'dit-spending-over-25000-may-2018',
                   pubs + 'dit-spending-over-25000-april-2018',
                   pubs + 'dit-spending-over-25000-march-2018',
                   pubs + 'dit-spending-over-25000-february-2018',
                   pubs + 'dit-spending-over-25000-january-2018',
                   pubs + 'dit-spending-over-25000-december-2017',
                   pubs + 'dit-spending-over-25000-november-2017',
                   pubs + 'department-for-international-trade-spend-2017-to-2018',
                   pubs + 'department-for-international-trade-spend-2016-to-2017']
        get_data(dataloc, filepath, dept)
    try:
        df = parse_data(filepath, dept)
        df.to_csv(os.path.join(filepath, '..', '..', 'output',
                               'mergeddepts', dept + '.csv'), index=False)
    except Exception as e:
        module_logger.debug('CRITICAL problem: Cannot construct a merged '
                            'output dataframe for ' + dept)


def dworkpen(filepath, dept):
    ''' Notes: Groupled publications in collection, but ends half through 2017
        How to update: check page at collections/dwp-payments-over-25-000
        Most recent file: Dec 2018
    '''
    createdir(filepath, dept)
    if 'noscrape' not in sys.argv:
        dataloc = [data + 'dataset/' + 'ccdc397a-3984-453b-a9d7-e285074bba4d/' +
                   'spend-over-25-000-in-the-department-for-work-and-pensions']
        get_data(dataloc, filepath, dept)
    try:
        df = parse_data(filepath, dept)
        df.to_csv(os.path.join(filepath, '..', '..', 'output',
                               'mergeddepts', dept + '.csv'), index=False)
    except Exception as e:
        module_logger.debug('CRITICAL problem: Cannot construct a merged '
                            'output dataframe for ' + dept)


def modef(filepath, dept):
    '''Notes: grouped £500 and £25000 together in one collection: super great.
    How to update: check collections/mod-finance-transparency-dataset
    Most recent file: Jan 2019
    '''
    createdir(filepath, dept)
    if 'noscrape' not in sys.argv:
        dataloc = [pubs + 'mod-spending-over-25000-january-to-december-2014',
                   pubs + 'mod-spending-over-25000-january-to-december-2015',
                   pubs + 'mod-spending-over-25000-january-to-december-2016',
                   pubs + 'mod-spending-over-25000-january-to-december-2017',
                   pubs + 'mod-spending-over-25000-january-to-december-2018',
                   pubs + 'mod-spending-over-25000-january-to-december-2019',
                   pubs + 'mod-spending-over-25000-january-to-december-2020',
                   pubs + 'mod-spending-over-25000-january-to-december-2021',
                   pubs + 'mod-spending-over-25000-january-to-december-2022',
                   pubs + 'mod-spending-over-25000-january-to-december-2023',
                   pubs + 'mod-spending-over-25000-january-to-december-2024',
                   ]
        get_data(dataloc, filepath, dept)
    df = parse_data(filepath, dept)
    df.to_csv(os.path.join(filepath, '..', '..', 'output',
                           'mergeddepts', dept + '.csv'), index=False)


def mojust(filepath, dept):
    '''Notes: because there are so many MOJ arms length bodies, the collections
    are a bit weird. Slightly outdated alsoself.
    How to update: search for a new landing page, something akin to:
    collections/moj-spend-over-25000-2018?
    Most recent file: Dec 2018
    '''
    createdir(filepath, dept)
    if 'noscrape' not in sys.argv:
        dataloc = [pubs + 'ministry-of-justice-spend-over-25000-2013',
                   pubs + 'ministry-of-justice-spend-over-25000-2014',
                   pubs + 'ministry-of-justice-spend-over-25000-2015',
                   pubs + 'ministry-of-justice-spend-over-25000-2016',
                   pubs + 'ministry-of-justice-spend-over-25000-2017',
                   pubs + 'ministry-of-justice-spending-over-25000-2018',
                   pubs + 'ministry-of-justice-spending-over-25000-2020',
                   pubs + 'ministry-of-justice-spending-over-25000-2021',
                   pubs + 'ministry-of-justice-spending-over-25000-2022',
                   pubs + 'ministry-of-justice-spending-over-25000-2023']
        get_data(dataloc, filepath, dept)
    try:
        df = parse_data(filepath, dept)
        df.to_csv(os.path.join(filepath, '..', '..', 'output',
                               'mergeddepts', dept + '.csv'), index=False)
    except Exception as e:
        module_logger.debug('CRITICAL problem: Cannot construct a merged '
                            'output dataframe for ' + dept)


def dcultmedsport(filepath, dept):
    ''' Notes: This is quite a mess -- each pubs page has a differet annual set
    How to update: search for a new landing page, something akin to:
    publications/dcms-transactions-over-25000-201819?
    Most recent file: Dec 2018
    '''
    createdir(filepath, dept)
    if 'noscrape' not in sys.argv:
        dataloc = [pubs + 'transactions-over-25k-2013-2014',
                   pubs + 'transactions-over-25000-august-2014',
                   pubs + 'transactions-over-25000-july-2014',
                   pubs + 'dcms-transactions-over-25000-2014-15',
                   pubs + 'dcms-transactions-over-25000-2015-16',
                   pubs + 'dcms-transactions-over-25000-201617',
                   pubs + 'dcms-transactions-over-25000-201718',
                   pubs + 'dcms-transactions-over-25000-201819',
                   pubs + 'dcms-transactions-over-25000-201920',
                   pubs + 'dcms-transactions-over-25000-202021',
                   pubs + 'dcms-transactions-over-25000-202122',
                   pubs + 'dcms-transactions-over-25000-202223',
                   pubs + 'dcms-transactions-over-25000-2023-to-2024']
        get_data(dataloc, filepath, dept)
    try:
        df = parse_data(filepath,
                        dept,
                        filestoskip=['transparency_report_-_transactions_over_25k_mar22.xlsx.xls']
                        )
        df.to_csv(os.path.join(filepath, '..', '..', 'output',
                               'mergeddepts', dept + '.csv'), index=False)
    except Exception as e:
        module_logger.debug('CRITICAL problem: Cannot construct a merged '
                            'output dataframe for ' + dept)


def ukexpfin(filepath, dept):
    ''' Notes: random files appear to be missing? good collection structure
    How to update: should be automatic? collections/ukef-spend-over-25-000
    Most recent file: Dec 2018
    '''
    createdir(filepath, dept)
    if 'noscrape' not in sys.argv:
        r = requests.get(base + 'collections/ukef-spend-over-25-000')
        htmllist = re.findall(
            "href=\"/government/publications/(.*?)\"\>", r.text)
        for htmlpage in htmllist:
            get_data([base + 'publications/' + htmlpage], filepath, dept)
    try:
        df = parse_data(filepath, dept)
        df.to_csv(os.path.join(filepath, '..', '..', 'output',
                               'mergeddepts', dept + '.csv'), index=False)
    except Exception as e:
        module_logger.debug('CRITICAL problem: Cannot construct a merged '
                            'output dataframe for ' + dept)


def dbusenind(filepath, dept):
    ''' Notes: very nice collection structure all on one pageself.
    How to update: collections/beis-spending-over-25000
    Most recent file: Sept 2017 (STILL! as of March 2019)'''
    createdir(filepath, dept)
    if 'noscrape' not in sys.argv:
        r = requests.get(base + 'collections/beis-spending-over-25000')
        htmllist = re.findall(
            "href=\"/government/publications/(.*?)\"\>", r.text)
        for htmlpage in htmllist:
            get_data([base + 'publications/' + htmlpage], filepath, dept)
    try:
        df = parse_data(filepath, dept, filestoskip=['beis-spending-over-25000-september-2021.csv'])
        df.to_csv(os.path.join(filepath, '..', '..', 'output',
                               'mergeddepts', dept + '.csv'), index=False)
    except Exception as e:
        module_logger.debug('CRITICAL problem: Cannot construct a merged '
                            'output dataframe for ' + dept)


def dfeeu(filepath, dept):
    ''' Notes: this is a bit of a mess... need to add files one by one?
    Most recent file: January 2019
    '''
    createdir(filepath, dept)
    if 'noscrape' not in sys.argv:
        lander = 'department-for-exiting-the-european-union-spend-over-25000-'
        dataloc = [pubs + 'department-for-exiting-the-european-union-expenditure-over-25000',
                   pubs + lander + 'november-2017-to-february-2018',
                   pubs + lander + 'march-2018',
                   pubs + lander + 'april-2018',
                   pubs + lander + 'may-2018',
                   pubs + lander + 'june-2018',
                   pubs + lander + 'july-2018',
                   pubs + lander + 'august-2018',
                   pubs + lander + 'september-2018',
                   pubs + lander + 'october-2018',
                   pubs + lander + 'november-2018',
                   pubs + lander + 'december-2018',
                   pubs + lander + 'january-2019',
                   pubs + lander + 'february-2019',
                   pubs + lander + 'march-2019',
                   pubs + lander + 'april-2019',
                   pubs + lander + 'may-2019',
                   pubs + lander + 'june-2019',
                   pubs + lander + 'july-2019',
                   pubs + lander + 'august-2018',
                   pubs + lander + 'september-2019',
                   pubs + lander + 'october-2019',
                   pubs + lander + 'november-2019',
                   pubs + lander + 'december-2019-and-january-2020'
                   ]
        get_data(dataloc, filepath, dept)
    try:
        df = parse_data(filepath, dept,
                        filestoskip = ['transparency_-_detail_2019_10__1_.xls',
                                       'transparency_-_detail_2019_11.xls'])
        df.to_csv(os.path.join(filepath, '..', '..', 'output',
                               'mergeddepts', dept + '.csv'), index=False)
    except Exception as e:
        module_logger.debug('CRITICAL problem: Cannot construct a merged '
                            'output dataframe for ' + dept)


def foroff(filepath, dept):
    ''' Note: great: everything in one collection pageself.
    How to update: should be automatic, but if not check:
    collections/foreign-office-spend-over-25000
    Most recent file: Jan 2018
    '''
    createdir(filepath, dept)
    if 'noscrape' not in sys.argv:
        r = requests.get(base + 'collections/foreign-office-spend-over-25000')
        htmllist = re.findall(
            "href=\"/government/publications/(.*?)\"\>", r.text)
        for htmlpage in htmllist:
            get_data([base + 'publications/' + htmlpage], filepath, dept)
    try:
        df = parse_data(filepath, dept,
                        filestoskip=['Publishable_November_2014_Spend.csv',
                                     'october_2013.csv'])
        df.to_csv(os.path.join(filepath, '..', '..', 'output',
                               'mergeddepts', dept + '.csv'), index=False)
    except Exception as e:
        module_logger.debug('CRITICAL problem: Cannot construct a merged '
                            'output dataframe for ' + dept)


def hmtreas(filepath, dept):
    ''' Note: out of date a bit, but collection is clean
    How to update: collections/25000-spend
    Most recent file: March 2017
    Note: No more recent file as of Sept 2018
    '''
    createdir(filepath, dept)
    if 'noscrape' not in sys.argv:
        r = requests.get(base + 'collections/25000-spend')
        htmllist = re.findall(
            "href=\"/government/publications/(.*?)\"\>", r.text)
        for htmlpage in htmllist:
            get_data([base + 'publications/' + htmlpage], filepath, dept)
    try:
        df = parse_data(filepath, dept)
        df.to_csv(os.path.join(filepath, '..', '..', 'output',
                               'mergeddepts', dept + '.csv'), index=False)
    except Exception as e:
        module_logger.debug('CRITICAL problem: Cannot construct a merged '
                            'output dataframe for ' + dept)


def mhclg(filepath, dept):
    ''' Note this changes from dclg in dec 2017.
    Note: therefore, grab all MHCLG only... this is a broken mess in general
    How to update: collections/mhclg-departmental-spending-over-250
    Most recent file: jan 2019
    '''
    createdir(filepath, dept)
    if 'noscrape' not in sys.argv:
        r = requests.get(
            base + 'collections/mhclg-departmental-spending-over-250')
        r1 = requests.get(base + 'collections/dluhc-spending-over-250')
        htmllist = re.findall(
            "ment/publications/(.*?)\" data-track", r.text)  # + \
        htmllist = [x for x in htmllist if ("procurement" not in x) and
                    ("card" not in x) and ("card" not in x)]
        for htmlpage in htmllist:
            get_data([base + 'publications/' + htmlpage], filepath, dept)
        htmllist = re.findall(
            "ment/publications/(.*?)\" data-track", r1.text)  # + \
        htmllist = [x for x in htmllist if ("procurement" not in x) and
                    ("card" not in x) and ("card" not in x)]
        for htmlpage in htmllist:
            get_data([base + 'publications/' + htmlpage], filepath, dept)
    try:
        df = parse_data(filepath, dept)
        df.to_csv(os.path.join(filepath, '..', '..', 'output',
                               'mergeddepts', dept + '.csv'), index=False)
    except Exception as e:
        module_logger.debug('CRITICAL problem: Cannot construct a merged '
                            'output dataframe for ' + dept)


def nioff(filepath, dept):
    '''Note: this data seems really, really out of date. Nothing on data.gov.uk
    How to update: publications/nio-transaction-spend-data-july-2011
    Most recent file: July 2011?
    '''
    createdir(filepath, dept)
    if 'noscrape' not in sys.argv:
        key = '7ea90d51-2ad6-4f28-bf25-e40341bc780e'
        landingpage = 'northern-ireland-office-nio-spending-over-25-000'
        dataloc = [data + 'dataset/' + key + '/' + landingpage]
        get_data(dataloc, filepath, dept, exclusions=[])
    try:
        df = parse_data(filepath, dept, filestoskip=['nio_s_headcount_and_payroll_data__january_2024.csv'])
        df.to_csv(os.path.join(filepath, '..', '..', 'output',
                               'mergeddepts', dept + '.csv'), index=False)
    except Exception as e:
        module_logger.debug('CRITICAL problem: Cannot construct a merged '
                            'output dataframe for ' + dept)


def download_file(url, save_path):
    response = requests.get(url)
    if response.status_code == 200:
        with open(save_path, 'wb') as file:
            file.write(response.content)
        time.sleep(2)


def waleoff(filepath, dept):
    createdir(filepath, dept)
    htmllist = ['https://www.gov.wales/sites/default/files/publications/2024-05/february-2024-expenditure-over-25k.ods',
                'https://www.gov.wales/sites/default/files/publications/2024-05/december-2023-expenditure-over-25k.ods',
                'https://www.gov.wales/sites/default/files/publications/2023-03/december-2022-expenditure-over-25k.ods',
                'https://www.gov.wales/sites/default/files/publications/2022-05/december-2021-expenditure-over-25k_0.ods'
                ]
    save_directory = os.path.join(filepath, dept)
    for url in htmllist:
        filename = os.path.basename(url)
        save_path = os.path.join(save_directory, filename)
        download_file(url, save_path)

    try:
        df = parse_data(filepath, dept, filestoskip=[])
        df.to_csv(os.path.join(filepath, '..', '..', 'output',
                               'mergeddepts', dept + '.csv'), index=False)
    except Exception as e:
        module_logger.debug('CRITICAL problem: Cannot construct a merged '
                            'output dataframe for ' + dept)
    #print('Files corrupted and fragmented for the Wales Office')


def leadercommons():
    print('No data for Office of the Leader of the House of Commons')


def leaderlords():
    print('No data for Office of the Leader of the House of Lords')


def scotoff(filepath, dept):
    '''Notes: this is really grim. have to manually scrape pages from the
    search function (eurgh)...
    How to update: manual search https://www.gov.uk/government/publications ?
    Most recent file: Feb 2019
    '''
    createdir(filepath, dept)
    if 'noscrape' not in sys.argv:
        base = 'https://www.ofgem.gov.uk/sites/default/files/'
        htmllist = [
            base + '2024/04/government-spend-over-gbp25000-november-2023/documents/government-spend-over-gbp25000-november-2023/government-spend-over-gbp25000-november-2023/govscot%3Adocument/Expenditure%2BPublication%2B-%2BNovember%2B2023%2B-%2BFinal%2Bfor%2BPublication.csv',
            base + '2024/04/government-spend-over-gbp25000-october-2023/documents/government-spend-over-gbp25000-october-2023/government-spend-over-gbp25000-october-2023/govscot%3Adocument/Expenditure%2BPublication%2B-%2BOctober%2B2023%2B-%2BFinal%2Bfor%2BPublication.csv',
            base + '2024/02/government-spend-over-gbp25000-september-2023/documents/government-spend-over-gbp25000-september-2023/government-spend-over-gbp25000-september-2023/govscot%3Adocument/Expenditure%2BPublication%2B-%2BSeptember%2B2023%2B-%2BFinal%2Bfor%2BPublication.csv',
            base + '2024/02/government-spend-over-gbp25000-august-2023/documents/government-spend-over-gbp25000-august-2023/government-spend-over-gbp25000-august-2023/govscot%3Adocument/Expenditure%2BPublication%2B-%2BAugust%2B2023%2B-%2BFinal%2Bfor%2BPublication.csv',
            base + '2024/02/government-spend-over-gbp25000-july-2023/documents/government-spend-over-gbp25000-july-2023/government-spend-over-gbp25000-july-2023/govscot%3Adocument/Expenditure%2BPublication%2B-%2BJuly%2B2023%2B-%2BFinal%2Bfor%2BPublication.csv',
            base + '2023/11/government-spend-over-gbp25000-june-2023/documents/government-spend-over-gbp25000-june-2023/government-spend-over-gbp25000-june-2023/govscot%3Adocument/Expenditure%2BPublication%2B-%2BJun%2B2023%2B-%2BFinal%2Bfor%2BPublication.csv',
            base + '2023/11/government-spend-over-gbp25000-may-2023/documents/government-spend-over-gbp25000-may-2023/government-spend-over-gbp25000-may-2023/govscot%3Adocument/Expenditure%2BPublication%2B-%2BMay%2B23%2B-%2BFinal%2Bfor%2BPublication.csv',
            base + '2023/11/government-spend-over-gbp25000-april-2023/documents/government-spend-over-gbp25000-april-2023/government-spend-over-gbp25000-april-2023/govscot%3Adocument/Expenditure%2BPublication%2B-%2BApr%2B2023%2B-%2BFinal%2Bfor%2BPublication.csv',
            base + '2023/08/government-spend-over-gbp25000-march-2023/documents/government-spend-over-gbp25000-march-2023/government-spend-over-gbp25000-march-2023/govscot%3Adocument/Expenditure%2BPublication%2B-%2BMarch%2B2023%2B-%2BFinal%2Bfor%2BPublication.csv',
            base + '2023/08/government-spend-over-gbp25000-february-2023/documents/government-spend-over-gbp25000-february-2023/government-spend-over-gbp25000-february-2023/govscot%3Adocument/Expenditure%2BPublication%2B-%2BFebruary%2B2023%2B-%2BFinal%2Bfor%2BPublication.csv',
            base + '2023/08/government-spend-over-gbp25000-january-2023/documents/government-spend-over-gbp25000-january-2023/government-spend-over-gbp25000-january-2023/govscot%3Adocument/Expenditure%2BPublication%2B-%2BJanuary%2B2023%2B-%2BFinal%2Bfor%2BPublication.csv',
            base + '2023/08/government-spend-over-gbp25000-december-2022/documents/government-spend-over-gbp25000-december-2022/government-spend-over-gbp25000-december-2022/govscot%3Adocument/Expenditure%2BPublication%2B-%2BDecember%2B2022%2B-%2BFinal%2Bfor%2BPublication2.csv',
            base + '2023/08/government-spend-over-gbp25000-november-2022/documents/government-spend-over-gbp25000-november-2022/government-spend-over-gbp25000-november-2022/govscot%3Adocument/Expenditure%2BPublication%2B-%2BNovember%2B2022%2B-%2BFinal%2Bfor%2BPublication.csv',
            base + '2023/08/government-spend-over-gbp25000-october-2022/documents/government-spend-over-gbp25000-october-2022/government-spend-over-gbp25000-october-2022/govscot%3Adocument/Expenditure%2BPublication%2B-%2BOctober%2B2022%2B-%2BFinal%2Bfor%2BPublication.csv',
            base + '2023/08/government-spend-over-gbp25000-september-2022/documents/government-spend-over-gbp25000-september-2022/government-spend-over-gbp25000-september-2022/govscot%3Adocument/Expenditure%2BPublication%2B-%2BSeptember%2B2022%2B-%2BFinal%2Bfor%2BPublication.csv',
            base + '2023/08/government-spend-over-gbp25000-august-2022/documents/government-spend-over-gbp25000-august-2022/government-spend-over-gbp25000-august-2022/govscot%3Adocument/Expenditure%2BPublication%2B-%2BAug%2B22%2B-%2BFinal%2Bfor%2BPublication.csv',
            base + '2023/08/government-spend-over-gbp25000-july-2022/documents/government-spend-over-gbp25000-july-2022/government-spend-over-gbp25000-july-2022/govscot%3Adocument/Expenditure%2BPublication%2B-%2BJuly%2B2022%2B-%2BFinal%2Bfor%2BPublication.csv',
            base + '2023/07/government-spend-over-gbp25000-june-2022/documents/government-spend-over-gbp25000-june-2022/government-spend-over-gbp25000-june-2022/govscot%3Adocument/Expenditure%2BPublication%2B-%2BJun%2B2022%2B-%2BFinal%2Bfor%2BPublication.csv.csv',
            base + '2023/07/government-spend-over-gbp25000/documents/government-spend-over-gbp25000-may-2022/government-spend-over-gbp25000-may-2022/govscot%3Adocument/Expenditure%2BPublication%2B-%2BMay%2B2022%2B-%2BFinal%2Bfor%2BPublication.csv.csv',
            base + '2023/07/government-spend-over-gbp25000-april-2022/documents/government-spend-over-gbp25000-april-2022/government-spend-over-gbp25000-april-2022/govscot%3Adocument/Expenditure%2BPublication%2B-%2BApril%2B2022%2B-%2BFinal%2Bfor%2BPublication.csv.csv',
            base + '2023/01/government-spend-over-gbp25000-march-2022/documents/government-spend-over-gbp25000-march-2022/government-spend-over-gbp25000-march-2022/govscot%3Adocument/Expenditure%2BPublication%2Bover%2B%25C2%25A325%252C000%2B-%2BMarch%2B2022.csv',
            base + '2023/01/government-spend-over-gbp25000-february-2022/documents/government-spend-over-gbp25000-february-2022/government-spend-over-gbp25000-february-2022/govscot%3Adocument/Expenditure%2BPublication%2Bover%2B%25C2%25A325%252C000%2B-%2BFebruary%2B2022.csv',
            base + '2023/01/government-spend-over-gbp25000-january-2022/documents/government-spend-over-gbp25000-january-2022/government-spend-over-gbp25000-january-2022/govscot%3Adocument/Expenditure%2BPublication%2Bover%2B%25C2%25A325%252C000%2B-%2BJanuary%2B2022%2B.csv',
            base + '2022/05/government-spend-over-gbp25000-december-2021/documents/government-spend-over-gbp25000-december-2021/government-spend-over-gbp25000-december-2021/govscot%3Adocument/Expenditure%2BPublication%2B-%2BDec%2B2021%2B-%2BFinal%2Bfor%2Bpublication%2Bversion.csv',
            base + '2022/05/government-spend-over-gbp25000-november-2021/documents/government-spend-over-gbp25000-november-2021/government-spend-over-gbp25000-november-2021/govscot%3Adocument/Expenditure%2BPublication%2B-%2BNov%2B2021%2B-%2BFinal%2Bfor%2Bpublication%2Bversion.csv',
            base + '2022/05/government-spend-over-gbp25000-october-2021/documents/government-spend-over-gbp25000-october-2021/government-spend-over-gbp25000-october-2021/govscot%3Adocument/Expenditure%2BPublication%2B-%2BOct%2B2021%2B-%2Bfinal%2Bfor%2Bpublication%2Bversion.csv',
            base + '2022/05/government-spend-over-gbp25000-september-2021/documents/government-spend-over-gbp25000-september-2021/government-spend-over-gbp25000-september-2021/govscot%3Adocument/Expenditure%2BPublication%2B-%2BSep%2B2021%2B-%2Bfinal%2Bfor%2Bpublication%2Bversion.csv',
            base + '2022/03/government-spend-over-gbp25000-august-2021/documents/government-spend-over-gbp25000-august-2021/government-spend-over-gbp25000-august-2021/govscot%3Adocument/Expenditure%2BPublication%2Bover%2B25k%2B-%2BAugust%2B2021%2B-%2BFinal%2Bfor%2BPublication.csv',
            base + '2022/03/government-spend-over-gbp25000-july-2021/documents/government-spend-over-gbp25000-july-2021/government-spend-over-gbp25000-july-2021/govscot%3Adocument/Expenditure%2BPublication%2Bover%2B25k%2B-%2BJuly%2B2021%2B-%2BFinal%2Bfor%2BPublication.csv',
            base + '2022/03/government-spend-over-gbp25000-june-2021/documents/government-spend-over-gbp25000-june-2021/government-spend-over-gbp25000-june-2021/govscot%3Adocument/Expenditure%2BPublication%2Bover%2B25k%2B-%2BJune%2B2021%2B-%2BFinal%2Bfor%2BPublication.csv',
            base + '2022/03/government-spend-over-gbp25000-may-2021/documents/government-spend-over-gbp25000-may-2021/government-spend-over-gbp25000-may-2021/govscot%3Adocument/Expenditure%2BPublication%2Bover%2B25k%2B-%2BMay%2B2021%2B-%2BFinal%2Bfor%2BPublication.csv',
            base + '2022/03/government-spend-over-gbp25000-april-2021/documents/government-spend-over-gbp25000-april-2021/government-spend-over-gbp25000-april-2021/govscot%3Adocument/Expenditure%2BPublication%2Bover%2B25k%2B-%2BApril%2B2021%2B-%2BFinal%2Bfor%2BPublication.csv',
            base + '2022/03/government-spend-over-gbp25000-march-2021/documents/government-spend-over-gbp25000-march-2021/government-spend-over-gbp25000-march-2021/govscot%3Adocument/Expenditure%2BPublication%2Bover%2B25k%2B-%2BMarch%2B2021%2B-%2BFinal%2Bfor%2BPublication.csv',
            base + '2021/11/government-spend-over-gbp25000-february-2021/documents/government-spend-over-gbp25000-february-2021/government-spend-over-gbp25000-february-2021/govscot%3Adocument/Expenditure%2BPublication%2Bover%2B25k%2B-%2BFebruary%2B2021%2B-%2BFinal%2Bfor%2BPublication.csv',
            base + '2021/11/government-spend-over-gbp25000-january-2021/documents/government-spend-over-gbp25000-january-2021/government-spend-over-gbp25000-january-2021/govscot%3Adocument/Expenditure%2BPublication%2Bover%2B25k%2B-%2BJanuary%2B2021%2B-%2BFinal%2Bfor%2BPublication.csv',
            base + '2021/08/government-spend-over-gbp25000-december-2020/documents/government-spend-over-gbp25000-december-2020/government-spend-over-gbp25000-december-2020/govscot%3Adocument/Expenditure%2BPublication%2Bover%2B25k%2B-%2BDecember%2B2020%2B-%2BFinal%2Bfor%2BPublication.csv',
            base + '2021/08/government-spend-over-gbp25000-november-2020/documents/government-spend-over-gbp25000-november-2020/government-spend-over-gbp25000-november-2020/govscot%3Adocument/Expenditure%2BPublication%2Bover%2B25k%2B-%2BNovember%2B2020%2B-%2BFinal%2Bfor%2BPublication.csv',
            base + '2021/07/government-spend-over-gbp25000-october-2020/documents/government-spend-over-gbp25000-october-2020/government-spend-over-gbp25000-october-2020/govscot%3Adocument/Expenditure%2BPublication%2B-%2BOct%2B2020%2B-%2BFinal%2Bfor%2Bpublication%2Bversion.csv',
            base + '2021/07/government-spend-over-gbp25000-september-2020/documents/government-spend-over-gbp25000-september-2020/government-spend-over-gbp25000-september-2020/govscot%3Adocument/Expenditure%2BPublication%2B-%2BSep%2B2020%2B-%2BFinal%2Bfor%2Bpublication%2Bversion.csv',
            base + '2021/02/government-spend-over-gbp25000-august-2020/documents/government-spend-over-gbp25000-august-2020/government-spend-over-gbp25000-august-2020/govscot%3Adocument/Expenditure%2BPublication%2Bover%2B25k%2B-%2BAugust%2B2020%2B-%2BFinal%2Bfor%2BPublication%2BVersion.csv',
            base + '2021/02/government-spend-over-gbp25000-july-2020/documents/government-spend-over-gbp25000-july-2020/government-spend-over-gbp25000-july-2020/govscot%3Adocument/Expenditure%2BPublication%2Bover%2B25k%2B-%2BJuly%2B2020%2B-%2BFinal%2BFor%2BPublication%2BVersion.csv',
            base + '2020/09/government-spend-over-gbp25000-june-2020/documents/government-spend-over-gbp25000-june-2020/government-spend-over-gbp25000-june-2020/govscot%3Adocument/Expenditure%2BPublication%2Bover%2B%25C2%25A325k%2B-%2BJune%2B2020%2B-%2BFinal%2Bfor%2BPublication.csv.csv',
            base + '2020/09/government-spend-over-gbp25000-may-2020/documents/government-spend-over-gbp25000-may-2020/government-spend-over-gbp25000-may-2020/govscot%3Adocument/Expenditure%2BPublication%2Bover%2B%25C2%25A325k%2B-%2BMay%2B2020%2B-%2BFinal%2Bfor%2BPublication.csv.csv',
            base + '2020/08/government-spend-over-gbp25000-april-2020/documents/government-spend-over-gbp25000-april-2020/government-spend-over-gbp25000-april-2020/govscot%3Adocument/Expenditure%2BPublication%2Bover%2B%25C2%25A325k%2B-%2BApril%2B2020%2B-%2BFinal%2BFor%2BPublication%2BVersion.csv',
            base + '2020/08/government-spend-over-gbp25000-march-2020/documents/government-spend-over-gbp25000-march-2020/government-spend-over-gbp25000-march-2020/govscot%3Adocument/Expenditure%2BPublication%2B-%2BMarch%2B2020%2B-%2BFinal%2BFor%2BPublication%2BVersion.csv',
            base + '2020/06/government-spend-over-gbp25000-february-20202/documents/government-spend-over-gbp25000-february-2020/government-spend-over-gbp25000-february-2020/govscot%3Adocument/Expenditure%2BPublication%2B-%2BFebruary%2B2020%2B-%2BFinal%2BFor%2BPublication%2BVersion.csv',
            base + '2020/06/government-spend-over-gbp25000-january-2020/documents/government-spend-over-gbp25000-january-2020/government-spend-over-gbp25000-january-2020/govscot%3Adocument/Expenditure%2BPublication%2B-%2BJanuary%2B2020%2B-%2BFinal%2BFor%2BPublication%2BVersion.csv',
            base + '2020/03/government-spend-over-gbp25000-december-2019/documents/government-spend-over-gbp25000-december-2019/government-spend-over-gbp25000-december-2019/govscot%3Adocument/Expenditure%2BPublication%2B-%2BDecember%2B2019%2B-%2Bfinal%2Bfor%2Bpublication%2Bversion.csv',
            base + '2020/03/government-spend-over-gbp25000-november-2019/documents/government-spend-over-gbp25000-november-2019/government-spend-over-gbp25000-november-2019/govscot%3Adocument/Expenditure%2BPublication%2B-%2BNovember%2B2019%2B-%2Bfinal%2Bfor%2Bpublication%2Bversion.csv',
            base + '2020/01/government-spend-over-gbp25000-october-2019/documents/government-spend-over-gbp25000-october-2019/government-spend-over-gbp25000-october-2019/govscot%3Adocument/Expenditure%2BPublication%2B-%2BOctober%2B2019%2B-%2Bfinal%2BVersion.csv',
            base + '2020/01/government-spend-over-gbp25000-september-2019/documents/government-spend-over-gbp25000-october-2019/government-spend-over-gbp25000-october-2019/govscot%3Adocument/Expenditure%2BPublication%2B-%2BSeptember%2B2019%2B-%2BFinal%2BVersion.csv',
            base + '2019/12/government-spend-over-gbp25000-august-2019/documents/government-spend-over-gbp25000-august-2019/government-spend-over-gbp25000-august-2019/govscot%3Adocument/Expenditure%2BPublication%2B-%2BAugust%2B2019%2B-%2Bfinal%2Bfor%2Bpublication.csv',
            base + '2019/12/government-spend-over-gbp25000-july-2019/documents/government-spend-over-gbp25000-july-2019/government-spend-over-gbp25000-july-2019/govscot%3Adocument/Expenditure%2BPublication%2B-%2BJuly%2B2019%2B-%2Bfinal%2Bfor%2Bpublication.csv',
            base + '2019/12/government-spend-over-gbp25000-june-2019/documents/government-spend-over-gbp25000-june-2019/government-spend-over-gbp25000-june-2019/govscot%3Adocument/Expenditure%2BPublication%2B-%2BJune%2B2019%2B-%2Bfinal.csv',
            base + '2019/12/government-spend-over-gbp25000-may-2019/documents/government-spend-over-gbp25000-may-2019/government-spend-over-gbp25000-may-2019/govscot%3Adocument/Expenditure%2BPublication%2B-%2BMay%2B2019%2B-%2Bfinal.csv',
            base + '2019/10/government-spend-over-gbp25000-april-2019/documents/government-spend-over-gbp25000-april-19/government-spend-over-gbp25000-april-19/govscot%3Adocument/Expenditure%2BPublication%2B-%2BApril%2B2019%2B-%2BFinal%2Bversion.csv',
            base + '2019/10/government-spend-over-gbp25000-march-2019/documents/government-spend-over-gbp25000-march-19/government-spend-over-gbp25000-march-19/govscot%3Adocument/Expenditure%2BPublication%2B-%2BMarch%2B2019%2B-%2Bfinal%2Bversion.csv',
            base + '2019/09/government-spend-over-gbp25000-february-2019/documents/government-spend-over-gbp25000-february-2019/government-spend-over-gbp25000-february-2019/govscot%3Adocument/Expenditure%2BPublication%2B-%2BFebruary%2B%2B2019-%2BFinal%2Bversion.csv',
            base + '2019/09/government-spend-over-gbp25000-january-2019/documents/government-spend-over-gbp25000-january-2019/government-spend-over-gbp25000-january-2019/govscot%3Adocument/Expenditure%2BPublication%2B-%2BJanuary%2B2019%2B-%2BFinal%2Bversion.csv',
            base + '2019/09/government-spend-over-gbp25000-december-2018/documents/government-spend-over-gbp25000-december-2018/government-spend-over-gbp25000-december-2018/govscot%3Adocument/Expenditure%2BPublication%2B-%2BDecember%2B2018%2B-%2BFInal%2Bversion.csv',
            base + '2019/09/government-spend-over-gbp25000-november-2018/documents/government-spend-over-gbp25000-november-2018/government-spend-over-gbp25000-november-2018/govscot%3Adocument/Expenditure%2BPublication%2B-%2BNovember%2B2018%2B-%2Bfinal%2Bversion.csv',
            base + '2019/05/government-spend-over-gbp25000-october-2018/documents/government-spend-over-gbp25000-october-2018/government-spend-over-gbp25000-october-2018/govscot%3Adocument/Expenditure%2BPublication%2B-%2BOctober%2B2018%2B-%2Bfinal%2Bversion.csv',
            base + '2019/05/government-spend-over-gbp25000-september-2018/documents/government-spend-over-gbp25000-september-2018/government-spend-over-gbp25000-september-2018/govscot%3Adocument/Expenditure%2BPublication%2B-%2BSeptember%2B2018%2B-%2Bfinal%2Bversion.csv',
            base + '2019/05/government-spend-over-gbp25000-august-2018/documents/government-spend-over-gbp25k-august-2018/government-spend-over-gbp25k-august-2018/govscot%3Adocument/Expenditure%2BPublication%2B-%2BAugust%2B2018%2B-%2Bfinal%2Bversion.csv',
            base + '2019/04/government-spend-over-gbp25000-july-2018/documents/government-spend-over-gbp25000-july-2018/government-spend-over-gbp25000-july-2018/govscot%3Adocument/Expenditure%2BPublication%2B-%2BJuly%2B2018%2B-%2Bfinal%2Bversion.csv',
            base + '2019/04/government-spend-over-gbp25000-june-2018/documents/government-spend-over-gbp25000-june-2018/government-spend-over-gbp25000-june-2018/govscot%3Adocument/Expenditure%2BPublication%2B-%2BJune%2B2018%2B-%2Bfinal%2Bversion.csv',
            base + '2019/04/government-spend-over-gbp25000-may-2018/documents/government-spend-over-gbp25000-may-2018/government-spend-over-gbp25000-may-2018/govscot%3Adocument/Expenditure%2BPublication%2B-%2BMay%2B2018%2B-%2Bfinal%2Bversion.csv',
            base + '2019/04/government-spend-over-gbp25000-april-2018/documents/government-spend-over-gbp25000-april-2018/government-spend-over-gbp25000-april-2018/govscot%3Adocument/Expenditure%2BPublication%2B-%2BApril%2B2018%2B-%2Bfinal%2Bversion.csv',
            base + '2019/04/government-spend-over-gbp25000-march-2018/documents/government-spend-over-gbp25000-march-2018/government-spend-over-gbp25000-march-2018/govscot%3Adocument/Expenditure%2BPublication%2B-%2BMarch%2B2018%2B-%2Bfinal%2Bversion.csv',
            base + '2019/04/government-spend-over-gbp25000-february-2018/documents/government-spend-over-gbp25000---february-2018/government-spend-over-gbp25000---february-2018/govscot%3Adocument/Expenditure%2BPublication%2B-%2BFebruary%2B2018%2B-%2Bfinal%2Bversion.csv',
            base + '2019/03/government-spend-over-gbp25000-january-2018/documents/expenditure-over-gbp25k-january-2018/expenditure-over-gbp25k-january-2018/govscot%3Adocument/Expenditure%2BPublication%2B-%2BJanuary%2B2018%2B-%2BFinal%2Bfor%2BPublication.csv',
            base + '2019/03/government-spend-over-gbp25000---december-2017/documents/expenditure-over-gbp25k---december-2017/expenditure-over-gbp25k---december-2017/govscot%3Adocument/Expenditure%2BPublication%2B-%2BDecember%2B2017%2B-%2BFinal%2Bfor%2BPublication.csv',
            base + '2019/03/government-spend-over-gbp25000-november-2017/documents/expenditure-over-gbp25k---november-2017/expenditure-over-gbp25k---november-2017/govscot%3Adocument/Expenditure%2BPublication%2B-%2BNovember%2B2017%2B-%2BFinal%2Bfor%2BPublication.csv',
            base + '2018/12/government-spend-over-gbp25000-october-2017/documents/government-spend-over-gbp25k-october-2017/government-spend-over-gbp25k-october-2017/govscot%3Adocument/Government%2Bspend%2Bover%2B%25C2%25A325K%2BOctober%2B2017.csv',
            base + '2018/12/government-spend-over-gbp25000-september-2017/documents/government-spend-over-gbp25000---september-2017/government-spend-over-gbp25000---september-2017/govscot%3Adocument/Expenditure%2BPublication%2B-%2BSeptember%2B2017%2B-%2BFinal%2Bfor%2BPublication.csv',
            base + '2018/12/government-spend-over-gbp25000-august-2017/documents/government-spend-over-gbp25k-august-2017/government-spend-over-gbp25k-august-2017/govscot%3Adocument/Expenditure%2BPublication%2B-%2BAugust%2B2017%2B-%2BFinal%2Bfor%2Bpublication.csv',
            base + '2018/11/government-spend-over-25000-july-2017/documents/expenditure-over-gbp25k---july-2017/expenditure-over-gbp25k---july-2017/govscot%3Adocument/Expenditure%2Bover%2B%25C2%25A325k%2B-%2BJuly%2B2017.csv',
            base + '2018/11/government-spend-over-25000-june-2017/documents/government-spend-over-gbp25000-june-2017/government-spend-over-gbp25000-june-2017/govscot%3Adocument/Expenditure%2Bover%2B%25C2%25A325k%2B-%2BJune%2B2017.csv',
            base + '2018/11/government-spend-over-25000-may-2017/documents/expenditure-over-gbp25k---may-2017/expenditure-over-gbp25k---may-2017/govscot%3Adocument/Expenditure%2Bover%2B%25C2%25A325k%2B-%2BMay%2B2017.csv',
            base + '2018/06/government-spend-over-25000-april-2017/documents/expenditure-over-25k-april-2017-csv/expenditure-over-25k-april-2017-csv/govscot%3Adocument/Expenditure%2Bover%2B%25C2%25A325K%2B-%2BApril%2B2017.csv',
            base + '2018/06/government-spend-over-25000-march-2017/documents/expenditure-over-25k-march-2017-csv/expenditure-over-25k-march-2017-csv/govscot%3Adocument/Expenditure%2Bover%2B%25C2%25A325K%2B-%2BMarch%2B2017.csv',
            base + '2018/06/government-spend-over-25000-february-2017/documents/expenditure-over-25k-february-2017-csv/expenditure-over-25k-february-2017-csv/govscot%3Adocument/Expenditure%2Bover%2B%25C2%25A325K%2B-%2BFebruary%2B2017.csv',
            base + '2018/05/government-spend-over-25000-january-2017/documents/expenditure-publication-january-2017-csv/expenditure-publication-january-2017-csv/govscot%3Adocument/Expenditure%2BPublication%2B-%2BJanuary%2B2017.csv',
            base + '2018/05/government-spend-over-25000-december-2016/documents/expenditure-publication-december-2016-csv/expenditure-publication-december-2016-csv/govscot%3Adocument/Expenditure%2BPublication%2B-%2BDecember%2B2016.csv',
            base + '2018/05/government-spend-over-25000-november-2016/documents/expenditure-publication-november-2016-csv/expenditure-publication-november-2016-csv/govscot%3Adocument/Expenditure%2BPublication%2B-%2BNovember%2B2016.csv',
            base + '2017/01/government-spend-over-25000-october-2016/documents/25k-reporting-expenditure-publication-october-2016-csv/25k-reporting-expenditure-publication-october-2016-csv/govscot%3Adocument/25k%2Breporting%2B-%2BExpenditure%2BPublication%2B-%2BOctober%2B2016.csv',
            base + '2017/01/government-spend-over-25000-september-2016/documents/25k-reporting-expenditure-publication-september-2016-csv/25k-reporting-expenditure-publication-september-2016-csv/govscot%3Adocument/25k%2Breporting%2B-%2BExpenditure%2BPublication%2B-%2BSeptember%2B2016.csv',
            base + '2017/01/government-spend-over-25000-august-2016/documents/25k-reporting-expenditure-publication-august-2016-csv/25k-reporting-expenditure-publication-august-2016-csv/govscot%3Adocument/25k%2Breporting%2B-%2BExpenditure%2BPublication%2B-%2BAugust%2B2016.csv',
            base + '2017/01/government-spend-over-25000-july-2016/documents/25k-reporting-expenditure-publication-july-2016-csv/25k-reporting-expenditure-publication-july-2016-csv/govscot%3Adocument/25k%2Breporting%2B-%2BExpenditure%2BPublication%2B-%2BJuly%2B2016.csv',
            base + '2016/12/government-spend-over-25000-june-2016/documents/25k-reporting-expenditure-publication-jun-2016-final-csv/25k-reporting-expenditure-publication-jun-2016-final-csv/govscot%3Adocument/%25C2%25A325K%2BReporting%2B-%2BExpenditure%2BPublication%2B-%2BJun%2B2016%2B-%2BFinal.csv',
            base + '2016/12/government-spend-over-25000-may-2016/documents/25k-reporting-expenditure-publication-2016-final-csv/25k-reporting-expenditure-publication-2016-final-csv/govscot%3Adocument/%25C2%25A325K%2BReporting%2B-%2BExpenditure%2BPublication%2B-%2BMay%2B2016%2B-%2BFinal.csv',
            base + '2016/12/government-spend-over-25000-april-2016/documents/25k-reporting-expenditure-publication-april-2016-final-csv/25k-reporting-expenditure-publication-april-2016-final-csv/govscot%3Adocument/%25C2%25A325K%2BReporting%2B-%2BExpenditure%2BPublication%2B-%2BApril%2B2016%2B-%2BFinal.csv',
            base + '2017/05/government-spend-over-25000-march-2016/documents/treasury-banking-25k-reporting-fy2015-16-mar-2016-csv/treasury-banking-25k-reporting-fy2015-16-mar-2016-csv/govscot%3Adocument/Treasury%2Band%2BBanking%2B-%2B%25C2%25A325K%2BReporting%2B-%2B%2528FY2015-16%2529%2B-%2BMar%2B2016.csv',
            base + '2016/10/government-spend-feb-2016/documents/treasury-banking-25k-reporting-fy2015-16-feb-2016-csv/treasury-banking-25k-reporting-fy2015-16-feb-2016-csv/govscot%3Adocument/Treasury%2Band%2BBanking%2B-%2B%25C2%25A325K%2BReporting%2B-%2B%2528FY2015-16%2529%2B-%2BFeb%2B2016.csv',
            base + '2016/06/government-spend-over-25000-january-2016/documents/sg-spend-over-25-000-jan-2016-csv/sg-spend-over-25-000-jan-2016-csv/govscot%3Adocument/SG%2Bspend%2Bover%2B%25C2%25A325%252C000%2B-%2BJan%2B2016.csv'
            ]
        save_directory = os.path.join(filepath, dept)
        for url in htmllist:
            filename = os.path.basename(url)
            save_path = os.path.join(save_directory, filename)
            download_file(url, save_path)
    try:
        df = parse_data(filepath, dept, filestoskip=[])
        df.to_csv(os.path.join(filepath, '..', '..', 'output',
                               'mergeddepts', dept + '.csv'), index=False)
    except Exception as e:
        module_logger.debug('CRITICAL problem: Cannot construct a merged '
                            'output dataframe for ' + dept)


def gldagohmcpsi(filepath, dept):
    ''' Notes: Data for Government Legal Department, Attorney General’s Office
    and HM Crown Prosecution Service Inspectorate. The final link on the
    page is everything prior to march 2017
    How to update: collections/gld-ago-hmcpsi-transactions-greater-than-25000
    Most recent file: August 2018
    '''
    createdir(filepath, dept)
    if 'noscrape' not in sys.argv:
        r = requests.get(base + 'collections/' +
                         'gld-ago-hmcpsi-transactions-greater-than-25000')
        htmllist = re.findall(
            "href=\"/government/publications/(.*?)\"\>", r.text)
        htmllist.append('gld-ago-hmcpsi-transactions-over-25000-december-2019')
        htmllist.append('ago-gld-and-hmcpsi-gpc-transactions-over-25000-feb-to-mar-2023')
        htmllist.append('gld-ago-hmcpsi-transactions-over-25000-july-2019')
        htmllist.append('gld-ago-hmcpsi-transactions-over-25000-november-2019')
        htmllist.append('gld-ago-hmcpsi-transactions-over-25000-february-2019')
        htmllist.append('gld-ago-hmcpsi-transactions-over-25k-jul-oct-2021')
        htmllist.append('gld-ago-and-hmcpsi-transactions-over-25000-june-2021')
        htmllist.append('gld-ago-and-hmcpsi-transactions-over-25000-november-2020')
        htmllist.append('gld-ago-and-hmcpsi-transactions-over-25000-september-2020')
        htmllist.append('gld-ago-and-hmcpsi-transactions-over-25000-april-2021')
        htmllist.append('gld-ago-and-hmcpsi-transactions-over-25000-january-2021')
        htmllist.append('gld-ago-and-hmcpsi-transactions-over-25000-october-2020')
        htmllist.append('gld-ago-and-hmcpsi-transactions-over-25000-may-2021')
        htmllist.append('gld-ago-and-hmcpsi-transactions-over-25000-december-2020')
        htmllist.append('gld-ago-and-hmcpsi-transactions-over-25000-march-2021')
        htmllist.append('gld-ago-and-hmcpsi-transactions-over-25000-may-2020')
        htmllist.append('gld-ago-and-hmcpsi-transactions-over-25000-february-2021')
        htmllist.append('gld-ago-and-hmcpsi-transactions-over-25000-august-2020')
        htmllist.append('gld-ago-and-hmcpsi-transactions-over-25000-april-2020')
        htmllist.append('gld-ago-and-hmcpsi-transactions-over-25000-june-2020')
        htmllist.append('gld-ago-and-hmcpsi-transactions-over-25000-july-2020')
        htmllist.append('gld-ago-hmcpsi-transactions-over-25000-october-2019')
        htmllist.append('ago-gld-and-hmcpsi-gpc-transactions-over-25000-jun-2022-jan-2023')
        htmllist.append('gld-ago-hmcpsi-transactions-over-25000-september-2019')
        htmllist.append('gld-ago-and-hmcpsi-transactions-over-25000-january-2020')
        htmllist.append('gld-ago-and-hmcpsi-transactions-over-25000-march-2020')
        htmllist.append('gld-ago-hmcpsi-transactions-over-25000-august-2019')
        htmllist.append('gld-ago-and-hmcpsi-transactions-over-25000-february-2020')
        for htmlpage in htmllist:
            get_data([base + 'publications/' + htmlpage], filepath, dept)
    try:
        df = parse_data(filepath, dept)
        df.to_csv(os.path.join(filepath, '..', '..', 'output',
                               'mergeddepts', dept + '.csv'), index=False)
    except Exception as e:
        module_logger.debug('CRITICAL problem: Cannot construct a merged '
                            'output dataframe for ' + dept)


def homeoffice(filepath, dept):
    '''Note: all there, but gotta go into each annual page, not updated recently
    How to update: publications/home-office-spending-over-25000-2018 ?
    Most recent file: Jan 2017.
    Interesting -- probably dont expect anything off them any time soon:
    https://ico.org.uk/media/action-weve-taken/decision-notices/2018/2258292/fs50694249.pdf
    '''
    createdir(filepath, dept)
    if 'noscrape' not in sys.argv:
        r = requests.get(base + 'collections/25000-spend')
        htmllist = re.findall(
            "href=\"/government/publications/(.*?)\"\>", r.text)
        for htmlpage in htmllist:
            get_data([base + 'publications/' + htmlpage], filepath, dept)
    try:
        df = parse_data(filepath, dept, filestoskip=['april-2011.xls',
                                                     'HO_GLAA_25K_Spend_2018_ODS.ods',
                                                     'HO_GLAA_25K_Spend_2018_CSV.csv'])
        df.to_csv(os.path.join(filepath, '..', '..', 'output',
                               'mergeddepts', dept + '.csv'), index=False)
    except Exception as e:
        module_logger.debug('CRITICAL problem: Cannot construct a merged '
                            'output dataframe for ' + dept)


def oags(filepath, dept):
    '''Note: this is a mess. old files are part of a big collection, but new
    files are randomly scattered across the pubs subdomain? Some random files
    are also missing.
    How to update: manual search https://www.gov.uk/government/publications ?
    Most recent file: Feb 2018?
    '''
    createdir(filepath, dept)
    if 'noscrape' not in sys.argv:
        land = 'oag-spending-over-25000-for-'
        htmllist = [
                    land + 'may-2024',
                    land + 'april-2024',
                    land + 'march-2024',
                    land + 'february-2024',
                    land + 'january-2024',

                    land + 'december-2023',
                    land + 'november-2023',
                    land + 'october-2023',
                    land + 'september-2023',
                    land + 'august-2023',
                    land + 'july-2023',
                    land + 'june-2023',
                    land + 'may-2023',
                    land + 'april-2023',
                    land + 'march-2023',
                    land + 'february-2023',
                    land + 'january-2023',

                    land + 'december-2022',
                    land + 'november-2022',
                    land + 'october-2022',
                    land + 'september-2022',
                    land + 'august-2022',
                    land + 'july-2022',
                    land + 'june-2022',
                    land + 'may-2022',
                    land + 'april-2022',
                    land + 'march-2022',
                    land + 'february-2022',
                    land + 'january-202',

                    land + 'december-2021',
                    land + 'november-2021',
                    land + 'october-2021',
                    land + 'september-2021',
                    land + 'august-2021',
                    land + 'july-2021',
                    land + 'june-2021',
                    land + 'may-2021',
                    land + 'april-2021',
                    land + 'march-2021',
                    land + 'february-2021',
                    land + 'january-2021',

                    land + 'december-2020',
                    land + 'november-2020',
                    land + 'october-2020',
                    land + 'september-2020',
                    land + 'august-2020',
                    land + 'july-2020',
                    land + 'june-2020',
                    land + 'may-2020',
                    land + 'april-2020',
                    land + 'march-2020',
                    land + 'february-2020',
                    land + 'january-2020',

                    land + 'december-2019',
                    land + 'november-2019',
                    land + 'october-2019',
                    land + 'september-2019',
                    land + 'august-2019',
                    land + 'july-2019',
                    land + 'june-2019',
                    land + 'may-2019',
                    land + 'april-2019',
                    land + 'march-2019',
                    land + 'february-2019',
                    land + 'january-2019',

                    land + 'december-2018',
                    land + 'november-2018',
                    land + 'october-2018',
                    land + 'september-2018',
                    land + 'august-2018',
                    land + 'july-2018',
                    land + 'june-2018',
                    land + 'may-2018',
                    land + 'april-2018',
                    land + 'march-2018',
                    land + 'february-2018',
                    land + 'january-2018',

                    land + 'december-2017',
                    land + 'november-2017',
                    land + 'october-2017',
                    land + 'september-2017',
                    land + 'august-2017',
                    land + 'july-2017',
                    land + 'june-2017',
                    land + 'may-2017',
                    land + 'april-2017',
                    land + 'march-2017',
                    land + 'february-2017',
                    land + 'january-2017',

                    land + 'december-2016',
                    land + 'november-2016',
                    land + 'october-2016',
                    land + 'september-2016',
                    land + 'august-2016',
                    land + 'july-2016',
                    land + 'june-2016',
                    land + 'may-2016',
                    land + 'april-2016',
                    land + 'march-2016',
                    land + 'february-2016',
                    land + 'january-2016',

                    land + 'december-2015',
                    land + 'november-2015',
                    land + 'october-2015',
                    land + 'september-2015',
                    land + 'august-2015',
                    land + 'july-2015',
                    land + 'june-2015',
                    land + 'may-2015',
                    land + 'april-2015',
                    land + 'march-2015',
                    land + 'february-2015',
                    land + 'january-2015',

                    land + 'december-2014',
                    land + 'november-2014',
                    land + 'october-2014',
                    land + 'september-2014',
                    land + 'august-2014',
                    land + 'july-2014'
        ]
        for htmlpage in htmllist:
            get_data([base + 'publications/' + htmlpage], filepath, dept)
        r = requests.get(base + 'collections/spend-over-25-000')
        htmllist = re.findall(
            "href=\"/government/publications/(.*?)\"\>", r.text)
        for htmlpage in htmllist:
            get_data([base + 'publications/' + htmlpage], filepath, dept)
    try:
        df = parse_data(filepath,
                        dept,
                        filestoskip=['March_2017_-_Transparency_OAG.csv',
                                     'August_2016_-_Transparency-OAG.csv',
                                     'May_2013_OAG.csv',
                                     'Spend_over_25K_February_2013.xls',
                                     'May_2014_transparency_OAG.csv',
                                     'April_2014_transparency_OAG.csv',
                                     'Transparency-April-2013.csv',
                                     'Spend_over_25k_Apr_2011_-_Mar_2012.xls',
                                     'Spend_over_25k_August_2012.xls',
                                     'October_2017_-_Transparency_-_OAG.csv',
                                     'Nov_2014_transparency_OAG.csv',
                                     'Spend_over_25k_May_2012.xls',
                                     'Spend_over_25k_October_2012.xls',
                                     'August_2015_-_OAG.csv',
                                     'transparency_oag_aug_18.xlsx',
                                     'July_2015_-_Transparency_-_OAG.csv',
                                     'OAG_Transparency_Aug_2013.csv',
                                     'Nov_2015_-_Transparency-OAG.csv',
                                     'OAG_transparency_Sep_2013.csv',
                                     'July_2014_transparency_OAG.csv',
                                     'Sept_2016_-_Transparency-OAG.csv',
                                     'April_2015_-_Transparency-OAG.csv',
                                     'November_2017_-_Transparency_-_OAG.csv',
                                     'Sept_2015_-_Transparency-OAG.csv',
                                     'OAG_Transparency_Oct_2013.csv',
                                     'July_2017_-_Transparency_-_OAG.csv',
                                     'Spend_over_25k_July_2012.xls',
                                     'Jan_2016_-_Transparency-OAG.csv',
                                     'Spend_over_25k_June_2012.xls',
                                     'February_2017_-_Transparency_OAG.csv',
                                     'Transparency_March_2013.csv',
                                     'Transparency_-_OAG.csv',
                                     'Feb_2016_-_Transparency-OAG.csv',
                                     'Aug_2013_OAG_Transparency.csv',
                                     'September_2017_-_Transparency_-_OAG.csv',
                                     'December_2017_-_Transparency_-_OAG.csv',
                                     'Feb_2014_transparency_OAG.csv',
                                     'October_2014_-_Transparency_Report_-_Expenses.csv',
                                     'Aug_2014_transparency_OAG.csv',
                                     'June_2017_-_Transparency_-_OAG.csv',
                                     'Spend_over_25k_September_2012.xls',
                                     'June_2015_-_Transparency_-_OAG.csv',
                                     'June_2014_transparency_OAG.csv',
                                     'May_2016_-_Transparency-OAG.csv',
                                     'December_2016_-_Transparency_OAG.csv',
                                     'Spend_over_25k_November_2012.xls',
                                     'Dec_2013_Transparency.xlsx',
                                     'Nov_2013_Transparency.xlsx',
                                     'Dec_2014_Transparency_OAG.xlsx',
                                     'spend_over_25k_april_2012.xls',
                                     'spend_over_25k_december_2012.xls',
                                     'spend_over_25k_january_2013.xls',
                                     'spend_over_25k_sept_2010_-_mar_2011.xls'])
        df.to_csv(os.path.join(filepath, '..', '..', 'output',
                               'mergeddepts', dept + '.csv'), index=False)
    except Exception as e:
        module_logger.debug('CRITICAL problem: Cannot construct a merged '
                            'output dataframe for ' + dept)


def defra(filepath, dept):
    ''' Note: complete listing on data.gov.uk.
    How to update: automatic?: dataset/financial-transactions-data-defra
    Most recent file: January 2018
    '''
    createdir(filepath, dept)
    if 'noscrape' not in sys.argv:
        key = '91072f06-093a-41a2-b8b5-6f120ceafd62'
        landingpage1 = '/spend-over-25-000-in-the'
        landingpage2 = '-department-for-environment-food-and-rural-affairs'
        dataloc = [data + 'dataset/' + key + landingpage1 + landingpage2]
        get_data(dataloc, filepath, dept)
    try:
        df = parse_data(filepath, dept)
        df.to_csv(os.path.join(filepath, '..', '..', 'output',
                               'mergeddepts', dept + '.csv'), index=False)
    except Exception as e:
        module_logger.debug('CRITICAL problem: Cannot construct a merged '
                            'output dataframe for ' + dept)


def charcom(filepath, dept):
    ''' great: have to manually find pages in the search, slightly out of date
    How to update: manual search https://www.gov.uk/government/publications ?
    Most recent file: March 2017
    '''
    createdir(filepath, dept)
    if 'noscrape' not in sys.argv:
        dataloc = [
            base + '/publications/charity-commission-spending-over-25000-april-2023-to-march-2024',
            base + '/collections/charity-commission-spending-over-25000-april-2022-to-march-2023',
            base + '/collections/charity-commission-spending-over-25000-april-2021-to-march-2022',
            base + '/collections/charity-commission-spending-over-25000-march-2020-to-april-2021',
            base + '/collections/charity-commission-spending-over-25000-march-2019-to-april-2020',
            base + '/publications/charity-commission-spend-over-25000-2018-2019',
            base + '/publications/charity-commission-spend-over-25000-2017-2018',
            base + '/publications/invoices-over-25k-during-financial-year-2016-2017'
            ]
        get_data(dataloc, filepath, dept)
    try:
        df = parse_data(filepath, dept, filestoskip=['May11.csv',
                                                     'charity_commission_spending_over__25_000_december_2023.csv',
                                                     'charity_commission_spending_over__25_000_february_2024.csv',
                                                     'charity_commission_spending_over__25_000_january_2024.csv',
                                                     'charity_commission_spending_over__25_000_march_2024.csv',
                                                     'charity_commission_spending_over__25_000_november_2023.csv',
                                                     'charity_commission_spending_over__25_000_october_2023.csv'
                                                     ]
                        )
        df.to_csv(os.path.join(filepath, '..', '..', 'output',
                               'mergeddepts', dept + '.csv'), index=False)
    except Exception as e:
        module_logger.debug('CRITICAL problem: Cannot construct a merged '
                            'output dataframe for ' + dept)


def commarauth(filepath, dept):
    ''' this is a collection, so should update automatically
        Most recent file: Dec 2018
    '''

    createdir(filepath, dept)
    if 'noscrape' not in sys.argv:
        r = requests.get(base + 'collections/cma-spend-over-25000')
        htmllist = re.findall(
            "href=\"/government/publications/(.*?)\"\>", r.text)
        for htmlpage in htmllist:
            get_data([base + 'publications/' + htmlpage], filepath, dept)
    try:
        df = parse_data(filepath, dept,
                        filestoskip=['Payments_over__25k_October_17.ods',
                                     'spend-over-25k-june-2017.ods',
                                     'Payments_over__25k_September_17.ods',
                                     'spend-over-25k-may-2017.ods'])
        df.to_csv(os.path.join(filepath, '..', '..', 'output',
                               'mergeddepts', dept + '.csv'), index=False)
    except Exception as e:
        module_logger.debug('CRITICAL problem: Cannot construct a merged '
                            'output dataframe for ' + dept)
    # https://stackoverflow.com/questions/17834995/how-to-convert-opendocument-spreadsheets-to-a-pandas-dataframe


def crownprosser(filepath, dept):
    ''' Note: There is now a dedicated page when there wasnt previously
    Check that this actually works on the next runself, as it may be trying
    to parse really janky stuff (there are multiple files on the page...)

    Last update:
    '''
    createdir(filepath, dept)
    if 'noscrape' not in sys.argv:
        key = '22c9d6a0-2139-46c4-bf3d-5fe9722de873/'
        landingpage = 'spend-over-25-000-in-the-crown-prosecution-service'
        dataloc = [data + 'dataset/' + key + landingpage]
        get_data(dataloc, filepath, dept, exclusions=[])
    try:
        df = parse_data(filepath, dept,
                        filestoskip=[''])
        df.to_csv(os.path.join(filepath, '..', '..', 'output',
                               'mergeddepts', dept + '.csv'), index=False)
    except Exception as e:
        module_logger.debug('CRITICAL problem: Cannot construct a merged '
                            'output dataframe for ' + dept)


def fsa(filepath, dept):
    ''' some of the files requested are links to other sites and are returning
    html: but these dont get parsed so just ignore them for now,

    Last update: October 2018'''
    createdir(filepath, dept)
    if 'noscrape' not in sys.argv:
        dataloc = [data + 'dataset/ba7e9e48-1f7e-4364-a076-28d3baf4493a/spend-over-25-000-in-the-food-standards-agency']
        get_data(dataloc, filepath, dept)
    try:
        df = parse_data(filepath, dept, filestoskip=['fsa-spend-aug2013.csv',
                                                     'fsa-spend-jan-2015.xls'
                                                     ]
                        )
        df.to_csv(os.path.join(filepath, '..', '..', 'output',
                               'mergeddepts', dept + '.csv'), index=False)
    except Exception as e:
        module_logger.debug('CRITICAL problem: Cannot construct a merged '
                            'output dataframe for ' + dept)


def forcomm(filepath, dept):
    ''' Fixed August 2018 to instead go direct to forestry.gov and get data
    from there. A few of the files are duplicated across months but they
    appear to be for different things, i.e.
        April 2018 Forestry Commisson England (CSV File)
        April 2018 Forest Enterprise England (CSV File)
    but these are just executuve agencies?
    last updated: June 2018'''

    createdir(filepath, dept)
    if 'noscrape' not in sys.argv:
        dataloc = [data + 'dataset/7189ef00-0d1e-436e-bdb5-181519bccead/spend-over-25-000-in-the-forestry-commission']
        get_data(dataloc, filepath, dept)
    try:
        df = parse_data(filepath, dept)
        df.to_csv(os.path.join(filepath, '..', '..', 'output',
                               'mergeddepts', dept + '.csv'), index=False)
    except Exception as e:
        module_logger.debug('CRITICAL problem: Cannot construct a merged '
                            'output dataframe for ' + dept)


def govlegdep():
    print('Data for GLD is merged with the AGO.')


def govaccdept(filepath, dept):
    ''' Note: each year has its own publications page, pretty janky
    How to update: search for 'gad-spend-greater-than-25000-2018?'?
    Most recent file: Jan 2019'''
    createdir(filepath, dept)
    if 'noscrape' not in sys.argv:
        dataloc = [pubs + 'gad-spend-greater-than-25000-2014',
                   pubs + 'gad-spend-greater-than-25000-2015',
                   pubs + 'gad-spend-greater-than-25000-2016',
                   pubs + 'gad-spend-greater-than-25000-2017',
                   pubs + 'gad-spend-greater-than-25000-2018',
                   pubs + 'gad-spend-greater-than-25000-2018',
                   pubs + 'gad-spend-greater-than-25000-2019',
                   pubs + 'government-actuarys-department-gad-spending-over-25000-2020',
                   pubs + 'government-actuarys-department-gad-spending-over-25000-2021',
                   pubs + 'government-actuarys-department-gad-spending-over-25000-2022',
                   pubs + 'government-actuarys-department-gad-spending-over-25000-2023',
                   pubs + 'government-actuarys-department-gad-spending-over-25000-2024',
                   ]
        get_data(dataloc, filepath, dept)
    try:
        df = parse_data(filepath, dept, filestoskip=['GAD_Nov_2016__25k_.csv'])
        df.to_csv(os.path.join(filepath, '..', '..', 'output',
                               'mergeddepts', dept + '.csv'), index=False)
    except Exception as e:
        module_logger.debug('CRITICAL problem: Cannot construct a merged '
                            'output dataframe for ' + dept)


def hmlandreg(filepath, dept):
    ''' Notes: exemplary! all in one collections page.
    How to update: should be automatic, if not, check:
    collections/land-registry-expenditure-over-25000
    Most recent file: November 2018
    '''
    createdir(filepath, dept)
    if 'noscrape' not in sys.argv:
        r = requests.get(
            base + 'collections/land-registry-expenditure-over-25000')
        htmllist = re.findall(
            "href=\"/government/publications/(.*?)\"\>", r.text)
        for htmlpage in htmllist:
            get_data([base + 'publications/' + htmlpage], filepath, dept)
    try:
        df = parse_data(filepath, dept)
        df.to_csv(os.path.join(filepath, '..', '..', 'output',
                               'mergeddepts', dept + '.csv'), index=False)
    except Exception as e:
        module_logger.debug('CRITICAL problem: Cannot construct a merged '
                            'output dataframe for ' + dept)


def hmrc(filepath, dept):
    ''' Notes: exemplary! all in one collections page.
    How to update: should be automatic, if not, check:
    collections/spending-over-25-000
    Most recent file: Jan 2018
    '''
    createdir(filepath, dept)
    if 'noscrape' not in sys.argv:
        r = requests.get(base + 'collections/25000-spend')
        htmllist = re.findall(
            "href=\"/government/publications/(.*?)\"\>", r.text)
        for htmlpage in htmllist:
            get_data([base + 'publications/' + htmlpage],
                     filepath, dept, exclusions=['RCDTS'])
    try:
        df = parse_data(filepath, dept)
        df.to_csv(os.path.join(filepath, '..', '..', 'output',
                               'mergeddepts', dept + '.csv'), index=False)
    except Exception as e:
        module_logger.debug('CRITICAL problem: Cannot construct a merged '
                            'output dataframe for ' + dept)


def natsavinv(filepath, dept):
    '''Note: have to devise a less general function to visit third party website
    How to update: should be automatic, if not check the main landing page
    Most recent file: February 2018
    '''

    createdir(filepath, dept)
    if 'noscrape' not in sys.argv:
        base = 'https://nsandi-corporate.com/sites/default/files/'
        htmllist = [
            base + '2024-04/NSI%20-%20Transparency%20Report%20January%202024%20-%20invoices%20over%2025K-Final%20file.csv',
            base + '2024-04/NSI%20-%20Transparency%20Report%20December%202023%20-Main%20File1invoices%20over%2025%20K.csv',
            base + '2024-01/NSI%20-%20Transparency%20Report%20November%202023%20-%20invoices%20over%2025K-Final%20file.csv',
            base + '2024-04/NSI%20-%20Transparency%20Report%20October%202023%20-Main%20File1invoices%20over%2025%20K%20.csv',
            base + '2023-11/Transparency%20report%20september%202023%20-%20invoices%20over%2025k%20final.csv',
            base + '2023-09/NSI%20-%20Transparency%20Report%20July%202023%20-%20invoices%20over%2025K-Final%20file.csv',
            base + '2023-07/NSI%20-%20Transparency%20Report%20May%202023%20-%20invoices%20over%2025K-Final%20file.csv',
            base + '2023-06/NSI%20-%20Transparency%20Report%20April%202023%20-%20invoices%20over%2025K-Final%20file.csv',
            base + '2023-06/NSI%20-%20Transparency%20Report%20March%202023%20-%20invoices%20over%2025K-Final%20file.csv',
            base + '2023-04/NSI%20-%20Transparency%20Report%20February%202023%20-%20invoices%20over%2025K-Final%20file.csv',
            base + '2023-02/NSI%20-%20Transparency%20Report%20January%202023%20-%20invoices%20over%2025K-Final%20file.csv',
            base + '2023-01/NSI%20-%20Transparency%20Report%20December%202022%20-%20invoices%20over%2025K-Final%20file.csv',
            base + '2022-12/NSI%20-%20Transparency%20Report%20November%202022%20-%20invoices%20over%2025K-Final%20file.csv',
            base + 'NSI%20-%20Transparency%20Report%20October%202022%20-%20invoices%20over%2025K-Final%20file.csv',
            base + '2022-10/NSI%20-%20Transparency%20Report%20September%202022%20-%20invoices%20over%2025K-Final%20file.csv',
            base + '2022-10/NSI%20-%20Transparency%20Report%20August%202022%20-%20invoices%20over%2025K-Final%20file.csv',
            base + '2022-09/Transparency%20Report%20July%202022%20-%20invoices%20over%2025K-Final%20file.csv',
            base + '2022-07/NSI%20-%20Transparency%20Report%20June%202022%20-%20invoices%20over%2025K-Final%20file.csv',
            base + '2022-06/May%2022%20Transparency%20Report%20-%20invoices%20over%2025K.csv',
            base + '2022-06/NSI%20-%20April%202022%20Transparency%20Report%20-%20invoices%20over%2025K-Final.csv',
            base + '2022-05/NSI%20-%20March%202022%20Transparency%20Report%20-%20invoices%20over%2025K.csv',
            base + '2022-03/NSI%20-February%202022%20Transparency%20Report%20-%20invoices%20over%2025K-Final.csv',
            base + '2022-03/NSI%20-January%202022%20Transparency%20Report%20-%20invoices%20over%2025K.csv',
            base + '2022-01/NSI%20-Dec%202021%20Transparency%20Report%20-%20invoices%20over%2025K-Final.csv',
            base + '2021-12/NSI%20-%20October%202021%20Transparency%20Report%20-%20invoices%20over%2025K.csv',
            base + '2021-10/nsi-september-transparency-report-invoices-over-25K..csv',
            base + '2021-10/NSI%20-%20August%202021%20Transparency%20Report%20-%20invoices%20over%2025K.csv',
            base + '2021-08/NSI%20-%20July%202021%20Transparency%20Report%20-%20invoices%20over%2025K.csv',
            base + '2021-07/transparency-report-invoices-over-25k-june-2021.csv',
            base + '2021-06/NSI%20-%20May%202021%20Transparency%20Report%20-%20invoices%20over%2025K.csv',
            base + '2021-06/NSI%20-%20April%202021%20Transparency%20Report%20-%20invoices%20over%2025K.csv',
            base + '2021-05/NSI%20-%20March%202021%20Transparency%20Report%20-%20invoices%20over%2025K.csv',
            base + '2021-04/NSI%20-%20Transparency%20Report%20-%20invoices%20over%2025K-Final%20File.230321.csv',
            base + '2021-02/january-2021-transparency-report-over-25k.csv',
            base + '2021-01/NSI%20-%20December%20Transparency%20Report%20-%20invoices%20over%2025K.csv',
            base + '2020-12/November%202020%20Transparency%20Report%20%C2%A325k.csv',
            base + '2020-11/oct-2020-transparency-report-invoices-over-25k-final.csv',
            base + '2020-10/september-2020-25k-transparency-report.csv',
            base + '2020-10/NSI%20-%20Transparency%20Report%20-%20invoices%20over%2025K-Main%20Aug%202020%20-Final.csv',
            base + '2020-08/july-2020-25k-transparency-report.csv',
            base + '2020-08/NSI%20-%20Transparency%20Report%20-%20invoices%20over%2025K%20%20Main%20source%20Final%20June%202020.csv',
            base + '2020-06/May%202020%20-%C2%A325k%20Transparency%20Report.csv',
            base + '2020-05/April%202020%20Transparency%20Report%20Source%20%20-%20Final%20version%20.csv',
            base + '2020-04/March%202020%20Transparency%20Report%20%C2%A325k.csv',
            base + '2020-04/february-2020-transparency-report.csv',
            base + '2020-02/january-2020-transparency-report-25k.csv',
            base + '2020-01/december-2019-transparency-report-source-final-version-14012020.csv',
            base + '2020-01/december-2019-transparency-report-source-final-version-14012020.csv'
        ]
        save_directory = os.path.join(filepath, dept)
        for url in htmllist:
            filename = os.path.basename(url)
            save_path = os.path.join(save_directory, filename)
            download_file(url, save_path)
    try:
        df = parse_data(filepath, dept, filestoskip=[])
        df.to_csv(os.path.join(filepath, '..', '..', 'output',
                               'mergeddepts', dept + '.csv'), index=False)
    except Exception as e:
        module_logger.debug('CRITICAL problem: Cannot construct a merged '
                            'output dataframe for ' + dept)


def natarch(filepath, dept):
    ''' Note: complete listing on data.gov.uk.
    How to update: automatic?: dataset/national-archives-items-of-spending
    Most recent file: Feb 2019
    '''
    createdir(filepath, dept)
    if 'noscrape' not in sys.argv:
        dataloc = [data + 'dataset/8413c306-0f2e-4905-837b-b60a72fa6551/the-national-archives-spend-over-10-000']
        get_data(dataloc, filepath, dept)
    try:
        df = parse_data(filepath, dept, filestoskip=[
            'april2013-spend-over10k.csv',
            'april-spend-over-10k.xls',
            'aug-spend-over-10k.xls',
            'spend-over-10k-june12.xls',
            'spend-over-10k-may15.xls'])
        df.to_csv(os.path.join(filepath, '..', '..', 'output',
                               'mergeddepts', dept + '.csv'), index=False)
    except Exception as e:
        module_logger.debug('CRITICAL problem: Cannot construct a merged '
                            'output dataframe for ' + dept)


def natcrimag():
    print('The National Crime Agency is exempt from FOI stuffs.')


def offrailroad(filepath, dept):
    '''Note: have to devise a less general function to visit third party website
    How to update: should be automatic, if not check the main landing page
    Most recent file: February 2018
    '''

    createdir(filepath, dept)
    if 'noscrape' not in sys.argv:
        base = 'https://nsandi-corporate.com/sites/default/files/'
        htmllist = [
            'https://www.orr.gov.uk/media/25430/download',
            'https://www.orr.gov.uk/media/25429/download',
            'https://www.orr.gov.uk/media/25428/download',
            'https://www.orr.gov.uk/media/25147/download',
            'https://www.orr.gov.uk/sites/default/files/2024-01/expenditure-over-25k-in-november-2023.csv',
            'https://www.orr.gov.uk/sites/default/files/2023-12/expenditure-over-25k-in-october-2023.csv',
            'https://www.orr.gov.uk/sites/default/files/2023-10/expenditure-over-25k-in-september-2023.csv',
            'https://www.orr.gov.uk/sites/default/files/2023-09/expenditure-over-25k-in-august-2023.csv',
            'https://www.orr.gov.uk/sites/default/files/2023-09/invoices-over-25k-July-2023.csv',
            'https://www.orr.gov.uk/sites/default/files/2023-09/invoices-over-25k-June-2023.csv',
            'https://www.orr.gov.uk/sites/default/files/2023-06/Invoices-over-25k-May-2023.csv',
            'https://www.orr.gov.uk/sites/default/files/2023-06/Invoices-over-25k-April-2023.csv',
            'https://www.orr.gov.uk/sites/default/files/2023-06/Invoices-over-25k-March-2023.csv',
            'https://www.orr.gov.uk/sites/default/files/2023-03/Invoices-over-25k-January-2023_0.csv',
            'https://www.orr.gov.uk/sites/default/files/2023-03/Invoices-over-25k-January-2023_0.csv',
            'https://www.orr.gov.uk/sites/default/files/2022-12/expenditure-over-25k-in-november-2022.csv',
            'https://www.orr.gov.uk/sites/default/files/2022-12/expenditure-over-25k-in-october-2022.csv',
            'https://www.orr.gov.uk/sites/default/files/2022-10/expenditure-over-25k-in-september-2022.csv',
            'https://www.orr.gov.uk/sites/default/files/2022-10/expenditure-over-25k-in-august-2022.csv',
            'https://www.orr.gov.uk/sites/default/files/2022-10/expenditure-over-25k-in-july-2022.csv',
            'https://www.orr.gov.uk/sites/default/files/2022-07/expenditure-over-25k-in-june-2022.csv',
            'https://www.orr.gov.uk/sites/default/files/2022-07/expenditure-over-25k-in-may-2022.csv',
            'https://www.orr.gov.uk/sites/default/files/2022-07/expenditure-over-25k-in-april-2022.csv',
            'https://www.orr.gov.uk/sites/default/files/2022-05/expenditure-over-25k-in-march-2022.csv',
            'https://www.orr.gov.uk/sites/default/files/2022-05/expenditure-over-25k-in-february-2022.csv',
            'https://www.orr.gov.uk/sites/default/files/2022-02/expenditure-over-25k-in-january-2022.csv',
            'https://www.orr.gov.uk/sites/default/files/2022-02/expenditure-over-25k-in-december-2021.csv',
            'https://www.orr.gov.uk/sites/default/files/2022-02/expenditure-over-25k-in-november-2021.csv',
            'https://www.orr.gov.uk/sites/default/files/2021-11/expenditure-over-25k-in-october-2021.csv',
            'https://www.orr.gov.uk/sites/default/files/2021-11/expenditure-over-25k-in-september-2021.csv',
            'https://www.orr.gov.uk/sites/default/files/2021-11/expenditure-over-25k-in-august-2021.csv',
            'https://www.orr.gov.uk/sites/default/files/2021-11/expenditure-over-25k-in-july-2021.csv',
            'https://www.orr.gov.uk/sites/default/files/2021-07/expenditure-in-june-2021.csv',
            'https://www.orr.gov.uk/sites/default/files/2021-08/expenditure-in-may-2021.csv',
            'https://www.orr.gov.uk/sites/default/files/2021-07/expenditure-in-april-2021.csv',
            'https://www.orr.gov.uk/sites/default/files/2021-07/expenditure-in-march-2021.csv',
            'https://www.orr.gov.uk/sites/default/files/2021-07/expenditure-in-february-2021.csv',
            'https://www.orr.gov.uk/sites/default/files/2021-02/expenditure-in-january-2021.csv',
            'https://www.orr.gov.uk/sites/default/files/2021-02/expenditure-in-december-2020.csv',
            'https://www.orr.gov.uk/sites/default/files/2021-02/expenditure-in-november-2020.csv',
            'https://www.orr.gov.uk/sites/default/files/2021-02/expenditure-in-october-2020.csv',
            'https://www.orr.gov.uk/sites/default/files/2020-10/expenditure-in-september-2020.csv',
            'https://www.orr.gov.uk/sites/default/files/2020-10/expenditure-in-august-2020.csv',
            'https://www.orr.gov.uk/sites/default/files/2020-10/expenditure-in-july-2020.csv',
            'https://www.orr.gov.uk/sites/default/files/2020-10/expenditure-in-june-2020.csv',
            'https://www.orr.gov.uk/media/11660/download',
            'https://www.orr.gov.uk/media/11659/download',
            'https://www.orr.gov.uk/media/11658/download',
            'https://www.orr.gov.uk/media/11639/download',
            'https://www.orr.gov.uk/media/11640/download',
            'https://www.orr.gov.uk/media/11641/download',
            'https://www.orr.gov.uk/media/11634/download',
            'https://www.orr.gov.uk/media/11631/download',
            'https://www.orr.gov.uk/media/11621/download',
            'https://www.orr.gov.uk/media/11623/download',
            'https://www.orr.gov.uk/media/11622/download',
            'https://www.orr.gov.uk/media/11616/download',
            'https://www.orr.gov.uk/media/11612/download',
            'https://www.orr.gov.uk/media/11607/download',
            'https://www.orr.gov.uk/media/11604/download',
            'https://www.orr.gov.uk/media/11602/download',
            'https://www.orr.gov.uk/media/11600/download',
            'https://www.orr.gov.uk/media/11599/download',
            'https://www.orr.gov.uk/media/11596/download',
            'https://www.orr.gov.uk/media/11588/download',
            'https://www.orr.gov.uk/media/11587/download',
            'https://www.orr.gov.uk/media/11583/download',
            'https://www.orr.gov.uk/media/11578/download',
            'https://www.orr.gov.uk/media/11574/download',
            'https://www.orr.gov.uk/media/11575/download',
            'https://www.orr.gov.uk/media/11581/download',
            'https://www.orr.gov.uk/media/11559/download',
            'https://www.orr.gov.uk/media/11560/download',
            'https://www.orr.gov.uk/media/11555/download',
            'https://www.orr.gov.uk/media/11556/download',
            'https://www.orr.gov.uk/media/11558/download',
            'https://www.orr.gov.uk/media/11554/download',
            'https://www.orr.gov.uk/sites/default/files/2023-11/spending-june-2017.csv',
            'https://www.orr.gov.uk/media/11540/download',
            'https://www.orr.gov.uk/media/11580/download',
            'https://www.orr.gov.uk/media/11521/download',
            'https://www.orr.gov.uk/media/11517/download',
            'https://www.orr.gov.uk/media/11516/download',
            'https://www.orr.gov.uk/media/11510/download',
            'https://www.orr.gov.uk/media/11509/download',
            'https://www.orr.gov.uk/media/11506/download',
            'https://www.orr.gov.uk/media/11505/download',
            'https://www.orr.gov.uk/media/11500/download',
            'https://www.orr.gov.uk/media/11497/download',
            'https://www.orr.gov.uk/media/11495/download',
            'https://www.orr.gov.uk/media/11496/download',
            'https://www.orr.gov.uk/media/11481/download',
            'https://www.orr.gov.uk/media/11579/download',
            'https://www.orr.gov.uk/media/11474/download',
            'https://www.orr.gov.uk/media/11473/download',
            'https://www.orr.gov.uk/media/11471/download',
            'https://www.orr.gov.uk/media/11472/download',
            'https://www.orr.gov.uk/media/11465/download',
        ]
        save_directory = os.path.join(filepath, dept)
        for url in htmllist:
            filename = os.path.basename(url)
            save_path = os.path.join(save_directory, filename)
            download_file(url, save_path)
    try:
        df = parse_data(filepath, dept, filestoskip=[])
        df.to_csv(os.path.join(filepath, '..', '..', 'output',
                               'mergeddepts', dept + '.csv'), index=False)
    except Exception as e:
        module_logger.debug('CRITICAL problem: Cannot construct a merged '
                            'output dataframe for ' + dept)


def ofgem(filepath, dept):
    ''' Note: custom function devised by the ofgem search function
    How to update: maybe add an extra page onto the range?
    Last update: April 2018?
    '''
    createdir(filepath, dept)
    if 'noscrape' not in sys.argv:
        base = 'https://www.ofgem.gov.uk/sites/default/files/'
        htmllist = [base + '2024-05/P11%20Payments%20to%20suppliers%20over%20%C2%A325k.xlsx',
                    base + '2024-05/P12%20Payments%20to%20suppliers%20over%20%C2%A325k.xlsx',
                    base + '2024-03/P9%20Supplier%20payment%20publication.xlsx',
                    base + '2024-02/P8%20Supplier%20payments%20over%20%C2%A325k%20publication_0.xlsx',
                    base + '2024-01/P7%20Supplier%20payments%20over%20%C2%A325k%20publication%2011.1.24.xlsx',
                    base + '2024-01/2023%20P6%20Supplier%20over%20%C2%A325k%20publication.xlsx',
                    base + '2023-11/2023%20P5%20Suppliers%20over%20%C2%A325k%20publication.xlsx',
                    base + '2023-10/2023%20P4%20Suppliers%20over%20%C2%A325k%20publication.xlsx',
                    base + '2023-09/24%20P3%20Supplier%20payments%20over%20%C2%A325k%20publication.xlsx',
                    base + '2023-07/P2%20Supplier%20payments%20over%20%C2%A325k.xlsx',
                    base + '2023-07/2023%20P12%20Over%20%C2%A325k%20publication.xlsx',
                    base + '2023-05/Supplier%20payments%20for%20Feb%2023.xlsx',
                    base + '2023-04/2023%20P10%20%C2%A325k%20publication.xlsx',
                    base + '2023-02/2023%20P9%20%C2%A325k%20publication_0.xlsx',
                    base + '2023-01/2023%20P8%20%C2%A325k%20publication_0.xlsx',
                    base + '2022-12/2023%20P7%20Over%20%C2%A325k%20publication.xlsx',
                    base + '2022-10/2023%20P6%20Over%20%C2%A325k%20publication%20-%20amended%20template.xlsx',
                    base + '2022-09/2023%20P5%20Over%20%C2%A325k%20publication%20%281%29.xlsx',
                    base + '2022-08/2023%20P4%20Over%20%C2%A325k%20publication.xlsx',
                    base + '2022-07/2023%20P2%2BP3%20Over%20%C2%A325k%20publication.xlsx',
                    base + '2022-05/2023%20P1%20Over%20%C2%A325K%20publication.xlsx',
                    base + '2022-04/2122%20P12%20Over%20%C2%A325K%20Publication.xlsx',
                    base + '2022-03/2122%20P11%20Over%20%C2%A325K%20Publication.xlsx',
                    base + '2022-02/2122%20P10%20Over%20%C2%A325K%20Publication.xlsx',
                    base + '2022-01/2122%20P9%20Over%20%C2%A325K%20Publication.xlsx',
                    base + '2021-12/2122%20P8%20Over%20%C2%A325K%20Publication.xlsx',
                    base + '2021-11/2122%20P7%20Over%20%C2%A325K%20Publication.xlsx',
                    base + '2021-10/P6%20Over%20%C2%A325K%20Publication.xlsx',
                    base + '2021-09/2122%20P5%20Over%20%C2%A325K%20Publication.xlsx',
                    base + '2021-08/2122%20P4%20Over%20%C2%A325K%20Publication.xlsx',
                    base + '2021-07/P3%20Over%20%C2%A325K%20Publication.xlsx',
                    base + '2021-07/2122%20P2%20Over%2025K%20Publication.xlsx',
                    base + 'docs/2021/05/2122_p1_over_25k_publication_april_2021_0.xlsx',
                    base + 'docs/2021/05/p12_over_25k_publication.xlsx',
                    base + 'docs/2021/03/over_ps25k_february_2021_publication_0.xlsx',
                    base + 'docs/2021/02/ofgem_over_ps25k_publication_-_jan_2021.xlsx',
                    base + 'docs/2021/01/p9_over_ps25k_publication.xlsx',
                    base + 'docs/2020/12/p8_over_ps25k_publication_for_november.xlsx',
                    base + 'docs/2020/11/over_25k_oct_2020_publication.xlsx',
                    base + 'docs/2020/10/over_25k_publication_document_sept_2020.xlsx',
                    base + 'docs/2020/09/ofgem_over_ps25k_publication_august_2020_0.xlsx',
                    base + 'docs/2020/09/over_25k_publication_july_2020_new_0.xlsx',
                    base + 'docs/2020/07/over_ps25_k_june_2020_publication.xlsx',
                    base + 'docs/2020/06/over_ps25k_april_20_-_may_20.xlsx',
                    base + 'docs/2020/05/payments_over_25k_publication_may_19_-_march_20_0.xlsx',
                    base + 'docs/2019/05/01_apr_2019_over_25k_spend_report.csv',
                    base + 'docs/2019/04/12_mar_2019_over_25k_spend_report.csv',
                    base + 'docs/2019/03/09_dec_2018_over_25k_spend_report.csv',
                    base + 'docs/2019/04/10_jan_2019_over_25k_spend_report_0.csv',
                    base + 'docs/2019/03/11_feb_2019_over_25k_spend_report.csv',
                    base + 'docs/2019/01/08_nov_2018_over_25k_spend_report.csv',
                    base + 'docs/2018/11/07_oct_2018_over_25k_spend_report.csv',
                    base + 'docs/2018/10/06_sep_2018_over_25k_spend_report.csv',
                    base + 'docs/2018/09/05_aug_2018_over_25k_spend_report.csv',
                    base + 'docs/2018/08/04_jul_2018_over_25k_spend_report.csv',
                    base + 'docs/2018/07/03_jun_2018_over_25k_spend_report.csv',
                    base + 'docs/2018/07/02_may_2018_over_25k_spend_report.csv',
                    base + 'docs/2018/05/01_apr_2018_over_25k_spend_report.csv',
                    base + 'docs/2018/05/07_oct_2017_over_25k_spend_report.csv',
                    base + 'docs/2018/05/08_nov_2017_over_25k_spend_report.csv',
                    base + 'docs/2018/05/09_dec_2017_over_25k_spend_report.csv',
                    base + 'docs/2018/05/10_jan_2018_over_25k_spend_report.csv',
                    base + 'docs/2018/05/11_feb_2018_over_25k_spend_report.csv',
                    base + 'docs/2018/05/12_mar_2018_over_25k_spend_report.csv',
                    base + 'docs/2017/10/06_sep_2017_over_25k_spend_report.csv',
                    base + 'docs/2017/09/03_jun_2017_over_25k_spend_report.csv',
                    base + 'docs/2017/09/04_jul_2017_over_25k_spend_report.csv',
                    base + 'docs/2017/09/05_aug_2017_over_25k_spend_report.csv',
                    base + 'docs/2017/05/12_mar_2017_over_25k_spend_report.csv',
                    base + 'docs/2017/05/01_apr_2017_over_25k_spend_report.csv',
                    base + 'docs/2017/06/02_may_2017_over_25k_spend_report.csv',
                    base + 'docs/2017/03/11_feb_2017_over_25k_spend_report.csv',
                    base + 'docs/2017/02/09_dec_2016_over_25k_spend_report.csv',
                    base + 'docs/2017/02/10_jan_2017_over_25k_spend_report.csv',
                    base + 'docs/2017/01/aug_2016_over_25k_spend_report.csv',
                    base + 'docs/2017/01/sep_2016_over_25k_spend_report.csv',
                    base + 'docs/2017/01/nov_2016_over_25k_spend_report.csv',
                    base + 'docs/2016/08/payments_to_suppliers_over_ps25000_july_2016_csv_version.csv',
                    base + 'docs/2016/07/payments_to_suppliers_over_ps25000_june_2016_csv_version.csv',
                    base + 'docs/2016/06/february_2016_over_25k_spend_report.csv',
                    base + 'docs/2016/06/march_2016_over_25k_spend_report.csv',
                    base + 'docs/2016/06/april_2016_over_25k_spend_report_0.csv',
                    base + 'docs/2016/06/may_2016_over_25k_spend_report.csv',
                    base + 'docs/2016/02/january_2016_over_25k_spend_report.csv',
                    base + 'docs/2016/02/august_2015_over_25k_spend_report.csv',
                    base + 'docs/2016/02/september_2015_over_25k_spend_report.csv',
                    base + 'docs/2016/02/october_2015_over_25k_spend_report.csv',
                    base + 'docs/2016/02/november_2015_over_25k_spend_report.csv',
                    base + 'docs/2018/12/dec_2015_over_25k_spend_report.csv',
                    base + 'docs/2015/08/july_2015_over_25k_spend_report_0.csv',
                    base + 'docs/2015/07/february_2015_over_25k_spend_report_0.csv',
                    base + 'docs/2015/07/march_2015_over_25k_spend_report_0.csv',
                    base + 'docs/2015/07/april_2015_over_25k_spend_report_0.csv',
                    base + 'docs/2015/07/may_2015_over_25k_spend_report.csv',
                    base + 'docs/2015/07/june_2015_over_25k_spend_report_0.csv',
                    base + 'docs/2015/02/november_2014_over_25k_spend_report.csv',
                    base + 'docs/2015/02/december_2014_over_25k_spend_report_0.csv',
                    base + 'docs/2015/02/january_2015_over_25k_spend_report_0.csv',
                    base + 'docs/2014/12/october_2014_over_25k_spend_report.csv',
                    base + 'docs/2014/10/september_2014_over_25k_spend_report_0.csv',
                    base + 'docs/2014/09/july_2014_over_25k_spend_report_0.csv',
                    base + 'docs/2014/09/august_2014_over_25k_spend_report_0.csv',
                    base + 'docs/2014/07/may_2014_over_25k_spend_report_0.csv',
                    base + 'docs/2014/07/june_2014_over_25k_spend_report_0.csv',
                    base + 'docs/2014/05/april_2014_over_25k_spend_report_0.csv',
                    base + 'docs/2014/04/february_2014_over_25k_spend_report_0.csv',
                    base + 'docs/2014/04/march_2014_over_25k_spend_report_0.csv',
                    base + 'docs/2014/03/january_2014_over_25k_spend_report_0.csv',
                    base + 'docs/2014/02/november_2013_over_25k_spend_report_0.csv',
                    base + 'docs/2014/02/december_2013_over_25k_spend_report_0.csv',
                    base + 'docs/2013/11/september_2013_over_25k_spend_report_0.csv',
                    base + 'docs/2013/11/october_2013_over_25k_spend_report.csv',
                    base + 'docs/2013/10/july_2013_over_25k_spend_report.csv',
                    base + 'docs/2013/10/august_2013_over_25k_spend_report.csv',
                    base + 'docs/2013/07/june-2013-over-25k-spend-report_1.csv',
                    base + 'docs/2013/07/may-2013-over-25k-spend-report.csv',
                    base + 'docs/2013/06/april-2013-over-25k-spend-report.csv',
                    base + 'docs/2013/04/march-2013-over-25k-spend-report.csv',
                    base + 'docs/2013/04/february-2013-over-25k-spend-report_0.csv',
                    base + 'docs/2013/02/january-2013-over-25k-spend-report_1.csv',
                    base + 'docs/2013/01/december-2012-over-25k-spend-report.csv',
                    base + 'docs/2012/12/november-2012-over-25k-spend-report_1.csv',
                    base + 'docs/2012/11/october-2012-over-25k-spend-report_1.csv',
                    base + 'docs/2012/10/sept-2012-over-25k-spend-report_1.csv',
                    base + 'docs/2012/10/august-2012-over-25k-spend-report_1.csv',
                    base + 'docs/2012/08/july-2012-over-25k-spend-report_1.csv',
                    base + 'docs/2012/07/june-2012-over-25k-spend-report_1.csv',
                    base + 'docs/2012/06/may-2012-over-25k-spend-report_1.csv',
                    base + 'docs/2012/05/april-2012-over-25k-spend-report_1.csv',
                    base + 'docs/2012/04/march-2012-over-25k-spend-report_1.csv',
                    base + 'docs/2012/04/february-2012-over-25k-spend-report_1.csv',
                    base + 'docs/2012/02/january-2012-over-25k-spend-report_1.csv',
                    base + 'docs/2012/01/december-2011-over-25k-spend-report_1.csv',
                    base + 'docs/2012/01/november-2011-over-25k-spend-report_1.csv',
                    base + 'docs/2011/11/october-2011-over-25k-spend-report_1.csv',
                    base + 'docs/2011/10/september-2011-over-25k-report_1.csv',
                    base + 'docs/2011/09/august-2011-over-25k-spend-report_1.csv',
                    base + 'docs/2011/09/july-2011-over-25k-spend-report_1.csv',
                    base + 'docs/2011/09/june-2011-over-25k-report_1.csv',
                    base + 'docs/2011/06/may-2011-over-25k-report_1.csv',
                    base + 'docs/2011/05/april-2011-over-25k-report_1.csv',
                    base + 'docs/2011/05/march-2011-over-25k-spend-report_1.csv',
                    base + 'docs/2011/03/25k-report-p11-february-2011_1.csv',
                    base + 'docs/2011/02/25k-report-p10-january-2011_1.csv',
                    base + 'docs/2011/02/25k-report-p9-december-2010_1.csv',
                    base + 'docs/2011/02/over-25k-report-nov-2010_1.csv',
                    base + 'docs/2011/02/25k-report-p7-october-2010_1.csv'
                    ]
        save_directory = os.path.join(filepath, dept)
        for url in htmllist:
            filename = os.path.basename(url)
            save_path = os.path.join(save_directory, filename)
            download_file(url, save_path)
    try:
        df = parse_data(filepath, dept, filestoskip=[])
        df.to_csv(os.path.join(filepath, '..', '..', 'output',
                               'mergeddepts', dept + '.csv'), index=False)
    except Exception as e:
        module_logger.debug('CRITICAL problem: Cannot construct a merged '
                            'output dataframe for ' + dept)


def ofqual(filepath, dept):
    '''Notes: Everything in one publications sheet
    How to update: automatic? publications/ofqual-spend-data-over-500
    Most recent file: 2018 to 2019
    '''
    createdir(filepath, dept)
    if 'noscrape' not in sys.argv:
        dataloc = [pubs + 'ofqual-spend-data-over-500']
        get_data(dataloc, filepath, dept)
    try:
        df = parse_data(filepath, dept,
                        filestoskip=['Ofqual_Expenditure_over_25k_May_2016.csv',
                                     'Ofqual_Expenditure_over_25k_January_2017.csv',
                                     'Ofqual_Expenditure_over_25k_November_2016.csv',
                                     'Ofqual_Expenditure_over_25k_July_2013.csv',
                                     'Ofqual_Expenditure_over_25k_October_2016.csv'])
        df.to_csv(os.path.join(filepath, '..', '..', 'output',
                               'mergeddepts', dept + '.csv'), index=False)
    except Exception as e:
        module_logger.debug('CRITICAL problem: Cannot construct a merged '
                            'output dataframe for ' + dept)


def ofsted(filepath, dept):
    '''Notes: four publications pages linked together via a collection
    How to update: collections/ofsted-spending-over-25000
    Most recent file: January 2019'''
    createdir(filepath, dept)
    if 'noscrape' not in sys.argv:
        dataloc = [pubs + 'ofsted-spending-over-25000-since-april-2010',
                   pubs + 'ofsted-spending-over-25000-2019',
                   pubs + 'ofsted-spending-over-25000-2016',
                   pubs + 'ofsted-spending-over-25000-2017',
                   pubs + 'ofsted-spending-over-25000-2018',
                   pubs + 'ofsted-spending-over-25000-2019',
                   pubs + 'ofsted-spending-over-25000-2020',
                   pubs + 'ofsted-spending-over-25000-2021',
                   pubs + 'ofsted-spending-over-25000-2022',
                   pubs + 'ofsted-spending-over-25000-in-2023',
                   pubs + 'ofsted-spending-over-25000-in-2024',
                   ]
        get_data(dataloc, filepath, dept)
    try:
        df = parse_data(filepath, dept,
                        filestoskip=['august_2021_spend_over_25k.ods',
                                     'january_2022_-__25k_transparency.ods',
                                     'july_2021_ofsted_spend_over__25k.ods',
                                     'july_2023_transparency_report__25k.ods',
                                     'june_2023_transparency_report_25k.ods',
                                     'k_transparency_report__january_2023.ods',
                                     'may_2023_transparency_report__25k.ods',
                                     'november_2023_25k_transparency_report.ods',
                                     'october_2021_transparency_report_for_publication.ods',
                                     'october_25k-transparency-report-2023.ods',
                                     'ofsted_25000_spend_december_2023.ods',
                                     'ofsted_25000_spend_january_2024.ods',
                                     'ofsted_25k_transparency_report_november_2022__.ods',
                                     'ofsted_august_2023_transparency_report_25k.ods',
                                     'ofsted_september_2021_spend_over__25k.ods',
                                     'ofsted_september_2023_transparency_report_25k.ods',
                                     'ofsted_spend_over_25k_october_2022.ods',
                                     'ofsted_transparency_25k_spend_december_2021.ods',
                                     'ofsted_transparency_25k_spend_november_2021.ods',
                                     'ofsted_transparency_report_25k_spend_march_2023.ods',
                                     'ofsted_transparency_report__25k_feb_2023.ods',
                                     'ofsted__25k_transparency_report_december_2022__1_.ods',
                                     'transparency_report_april_2022.ods',
                                     'transparency_report__25k_september_2022.ods'
                                     ],
                        )
        df.to_csv(os.path.join(filepath, '..', '..', 'output',
                               'mergeddepts', dept + '.csv'), index=False)
    except Exception as e:
        module_logger.debug('CRITICAL problem: Cannot construct a merged '
                            'output dataframe for ' + dept)


def serfraud(filepath, dept):
    ''' Custom site, but looks ok, maybe one day will have to add to the range:
    Last file: Sept 2018
    '''
    createdir(filepath, dept)
    if 'noscrape' not in sys.argv:
        base = 'https://www.sfo.gov.uk/download/'
        htmllist = [
            base + 'procurement-spend-over-25000-march-2024/?ind=1714482184160&filename=25k%20Report%20Mar%2024.xlsx&wpdmdl=37091&refresh=664f449a533e91716470938',
            base + 'procurement-spend-over-25000-february-2024/?ind=1714482128454&filename=25k%20Report%20Feb%2024.xlsx&wpdmdl=37088&refresh=664f449b4d64e1716470939',
            base + 'procurement-spend-over-25000-january-2024/?ind=1714482064510&filename=1714482064wpdm_25k%20Report%20Jan%2024.xlsx&wpdmdl=37086&refresh=664f449d3f89c1716470941',
            base + 'procurement-spend-over-25000-december-2023/?ind=1714481986213&filename=25k%20Report%20Dec%2023.xlsx&wpdmdl=37082&refresh=664f449e28b931716470942',
            base + 'procurement-spend-over-25000-november-2023/?ind=1714481900142&filename=25k%20Report%20Nov%2023.xlsx&wpdmdl=37080&refresh=664f449ee3eab1716470942',
            base + 'procurement-spend-over-25000-october-2023/?ind=1714481807621&filename=25k%20Report%20Oct%2023.xlsx&wpdmdl=37079&refresh=664f449fc16eb1716470943',
            base + 'procurement-spend-over-25000-september-2023/?ind=1700471551361&filename=%C2%A325k%20Report%20Sep%2023.xlsx&wpdmdl=36308&refresh=664f44a0b84391716470944',
            base + 'procurement-spend-over-25000-august-2023/?ind=1700471427903&filename=%C2%A325k%20Report%20Aug%2023.xlsx&wpdmdl=36302&refresh=664f44a19e2551716470945',
            base + 'procurement-spend-over-25000-july-2023/?ind=1697193719202&filename=%C2%A325k%20Report%20July%2023.xlsx&wpdmdl=36191&refresh=664f44aba22b41716470955',
            base + 'procurement-spend-over-25000-june-2023/?ind=1696250159888&filename=%C2%A325k%20Report%20June%2023.xlsx&wpdmdl=36098&refresh=664f44ace25731716470956',
            base + 'procurement-spend-over-25000-may-2023/?ind=1692265436733&filename=%C2%A325k%20report%20May%2023.xlsx&wpdmdl=35829&refresh=664f44ade42bc1716470957',
            base + 'procurement-spend-over-25000-april-2023/?ind=1688634465803&filename=%C2%A325k%20report%20Apr%2023.xlsx&wpdmdl=35705&refresh=664f44aedcf171716470958',
            base + 'procurement-spend-over-25000-march-2023/?ind=1688027870619&filename=25k%20Report%20Mar-23.xlsx&wpdmdl=35591&refresh=664f44afb561c1716470959',
            base + 'procurement-spend-over-25000-february-2023/?ind=1688027776501&filename=1688027776wpdm_25k%20Report%20Feb-23.xlsx&wpdmdl=35588&refresh=664f44b092ce71716470960',
            base + 'procurement-spend-over-25000-january-2023/?ind=1688027506540&filename=25K%20repor%20-%20Jan%2023%20-%20P10.xlsx&wpdmdl=35579&refresh=664f44b16c4831716470961',
            base + 'procurement-spend-over-25000-december-2022/?ind=1678195057455&filename=25k%20Report%20Dec-22.xlsx&wpdmdl=34937&refresh=664f44b2d0beb1716470962',
            base + 'procurement-spend-over-25000-november-2022/?ind=1675691888580&filename=%C2%A325k%20report%20Nov%2022.xlsx&wpdmdl=34872&refresh=664f44b5260981716470965',
            base + 'procurement-spend-over-25000-october-2022/?ind=1672846304601&filename=%C2%A325k%20report%20Oct-22.xlsx&wpdmdl=34753&refresh=664f44b62c4f31716470966',
            base + 'procurement-spend-over-25000-september-2022/?ind=1669981851083&filename=%C2%A325k%20report%20Sep%202022.xlsx&wpdmdl=34667&refresh=664f44b6e71671716470966',
            base + 'procurement-spend-over-25000-august-2022/?ind=1669981658637&filename=%C2%A325k%20report%20Aug%202022.xlsx&wpdmdl=34664&refresh=664f44b7952991716470967',
            base + 'procurement-spend-over-25000-july-2022/?ind=1669981485303&filename=25k%20July%2022.xlsx&wpdmdl=34662&refresh=664f44b8635291716470968',
            base + 'procurement-spend-over-25000-june-2022/?ind=1666170280200&filename=25k%20June%202022.xlsx&wpdmdl=34556&refresh=664f44b91355f1716470969',
            base + 'procurement-spend-over-25000-may-2022/?ind=1660310085591&filename=25k%20May%202022.xlsx&wpdmdl=34100&refresh=664f44ba18e7c1716470970',
            base + 'procurement-spend-over-25000-april-2022/?ind=1660309320733&filename=25k%20April%202022.xlsx&wpdmdl=34094&refresh=664f44baecafc1716470970',
            base + 'procurement-spend-over-25000-march-2022/?ind=1660308580816&filename=25k%20March%202022%20v1.xlsx&wpdmdl=34085&refresh=664f44bbe864f1716470971',
            base + 'procurement-spend-over-25000-february-2022/?ind=1660308336226&filename=1660308336wpdm_25k%20February%202022.xlsx&wpdmdl=34079&refresh=664f44bdb980a1716470973',
            base + 'procurement-spend-over-25000-january-2022/?ind=1655131871528&filename=25k%20January%202022%20v2.xlsx&wpdmdl=33542&refresh=664f44be946891716470974',
            base + 'procurement-spend-over-25000-december-2021/?ind=1645108840599&filename=25k%20December%202021.xlsx&wpdmdl=32902&refresh=664f44bf8da511716470975',
            base + 'procurement-spend-over-25000-november-2021/?ind=1645108476753&filename=25k%20paid%20invoices%20November%202021%20v2.xlsx&wpdmdl=32898&refresh=664f44c0357711716470976',
            base + 'procurement-spend-over-25000-september-2021/?ind=1638898829597&filename=25k%20paid%20invoices%20September%202021.csv&wpdmdl=32379&refresh=664f44c1ef9f01716470977',
            base + 'procurement-spend-over-25000-august-2021/?ind=1638898779207&filename=25k%20Aug%20-%202021%20v2.csv&wpdmdl=32376&refresh=664f44c3706951716470979',
            base + 'procurement-spend-over-25000-july-2021/?ind=1638898729011&filename=25k%20July%20-%202021.csv&wpdmdl=32373&refresh=664f44c432abf1716470980',
            base + 'procurement-spend-over-25000-june-2021/?ind=1631605153596&filename=Copy%20of%2025k%20June-2021.csv&wpdmdl=31704&refresh=664f44c538fc61716470981',
            base + 'procurement-spend-over-25000-may-2021/?ind=1631605135478&filename=25k%20May-2021.csv&wpdmdl=31709&refresh=664f44c6cba921716470982',
            base + 'procurement-spend-over-25000-march-2021/?ind=1623417685308&filename=25k%20MAR-2021.xlsx&wpdmdl=31046&refresh=664f44c81973f1716470984',
            base + 'procurement-spend-over-25000-february-2021/?ind=1623417470889&filename=25k%20February%202021%20v2.xlsx&wpdmdl=31044&refresh=664f44c8b4b2f1716470984',
            base + 'procurement-spend-over-25000-january-2021/?ind=1621601776037&filename=25k%20Report%20JAN-2021%20.xlsx&wpdmdl=30885&refresh=664f44c99b1361716470985',
            base + 'procurement-spend-over-25000-december-2020/?ind=1621601738978&filename=25k%20DEC-2020.xlsx&wpdmdl=30884&refresh=664f44ca8d98a1716470986',
            base + 'procurement-spend-over-25000-april-2021/?ind=1635230926610&filename=25k%20APRIL%202021.csv&wpdmdl=32130&refresh=664f44cb6aade1716470987',
            base + 'procurement-spend-over-25000-november-2020/?ind=1615296107871&filename=Copy%20of%2025k%20NOV-2020.csv&wpdmdl=29824&refresh=664f44cc0c5ec1716470988',
            base + 'procurement-spend-over-25000-october-2020/?ind=1615296047996&filename=Copy%20of%2025%20K%20report%20OCT-2020.csv&wpdmdl=29823&refresh=664f44e53102f1716471013',
            base + 'procurement-spend-over-25000-september-2020/?ind=1608029648964&filename=25k%20Report%20September-2020.csv&wpdmdl=28800&refresh=664f44e659ddd1716471014',
            base + 'procurement-spend-over-25000-august-2020/?ind=1608029463380&filename=25k%20Report%20August-2020.csv&wpdmdl=28797&refresh=664f44e73d4601716471015',
            base + 'procurement-spend-over-25000-july-2020/?ind=1608029463301&filename=25k%20Report%20July-2020.csv&wpdmdl=28794&refresh=664f44e8111301716471016',
            base + 'procurement-spend-over-25000-june-2020/?ind=1599142181385&filename=1599142182wpdm_25k%20Report%20June-2020.csv&wpdmdl=27620&refresh=664f44e8d009b1716471016',
            base + 'procurement-spend-over-25000-april-2020-2/?ind=1599149563248&filename=1599149564wpdm_25k%20Report%20APR-2020%20v2.csv&wpdmdl=27617&refresh=664f44e9c40ef1716471017',
            base + 'procurement-spend-over-25000-may-2020/?ind=1597228932000&filename=25k%20Report%20MAY-2020.csv&wpdmdl=27433&refresh=664f44eaef28c1716471018',
            base + 'procurement-spend-over-25000-march-2020/?ind=1594970847421&filename=25k%20Report%20MAR-2020%20v2.csv&wpdmdl=26676&refresh=664f44ebcb2db1716471019',
            base + 'procurement-spend-over-25000-february-2020/?ind=1592401044855&filename=25k%20Report%20FEB-2020%20v2.csv&wpdmdl=26673&refresh=664f44ecae3e91716471020',
            base + 'procurement-spend-over-25000-january-2020/?ind=1592400928087&filename=25k%20Report%20JAN-2020.csv&wpdmdl=26670&refresh=664f4753344101716471635',
            base + 'procurement-spend-over-25000-january-2020/?ind=1592400928087&filename=25k%20Report%20JAN-2020.csv&wpdmdl=26670&refresh=664f48c31b56a1716472003',
            base + 'procurement-spend-over-25000-november-2019/?ind=1589555750386&filename=Procurement%20Spend%20over%2025K%20-%20Nov%2019.csv&wpdmdl=26296&refresh=664f48d0aba471716472016',
            base + 'procurement-spend-over-25000-july-2019-2/?ind=1582728084293&filename=Procurement%20Spend%20over%2025,000%20July%202019.xlsx.csv&wpdmdl=25903&refresh=664f48d192e051716472017',
            base + 'procurement-spend-over-25000-july-2019/?ind=1582728042314&filename=Procurement%20Spend%20over%2025,000%20Aug%202019.csv&wpdmdl=25900&refresh=664f48d279ed61716472018',
            base + 'procurement-spend-over-25000-september-2019/?ind=1578662027864&filename=Procurement%20Spend%20over%20%C2%A3%2025K%20Sep-2019.csv&wpdmdl=25376&refresh=664f48d3870f61716472019',
            base + 'procurement-spend-over-25000-october-2019/?ind=1578661955919&filename=Procurement%20spend%20over%2025,000%20Spending%20October%202019%20.csv&wpdmdl=25375&refresh=664f48d475a6c1716472020',
            base + 'procurement-spend-over-25000-june-2019/?ind=1566999761550&filename=Procurement%20Spend%20over%2025,000%20June%202019.csv&wpdmdl=24308&refresh=664f48d556c771716472021',
            base + 'procurement-spend-over-25000-may-2019/?ind=1566999627235&filename=Procurement%20Spend%20over%2025000%20May%202019.csv&wpdmdl=24307&refresh=664f48d7002cb1716472023',
            base + 'procurement-spend-over-25000-april-2019/?ind=1566999540043&filename=Procurement%20Spend%20over%2025000%20Apr%202019.csv&wpdmdl=24306&refresh=664f48d7d070a1716472023',
            base + 'procurement-spend-over-25000-march-2019/?ind=1563377209697&filename=Procurement%20Spend%20over%2025000%20Mar-19.csv&wpdmdl=23927&refresh=664f48d8d5b621716472024',
            base + 'procurement-spend-over-25000-february-2019/?ind=1557214369116&filename=Procurement%20Spend%20over%2025000%20Feb%202019.csv&wpdmdl=23482&refresh=664f48d9c15821716472025',
            base + 'procurement-spend-over-25000-january-2019/?ind=1557214188128&filename=Procurement%20Spend%20over%2025000%20Jan%202019%20f.xlsx&wpdmdl=23476&refresh=664f48db5871a1716472027',
            base + 'procurement-spend-over-25000-november-2018/?ind=1554284523724&filename=Copy%20of%20Procurement%20Spend%20over%2025000%20Nov%202018.csv&wpdmdl=23213&refresh=664f48dc4a1f01716472028',
            base + '23209/?ind=1554275299145&filename=Procurement%20Spend%20over%2025,000%20Oct%202018.csv&wpdmdl=23209&refresh=664f48ddb78901716472029',
            base + 'procurement-spend-over-25000-december-2018/?ind=1553337454236&filename=Procurement%20Spend%20over%2025,000%20Dec%202018.csv&wpdmdl=23083&refresh=664f48dea09151716472030',
            base + 'procurement-spend-over-25000-september-2018/?ind=1553337088485&filename=Procurement%20Spend%20Over%20%C2%A325,000,%20September%202018.csv&wpdmdl=22555&refresh=664f48dfde94d1716472031',
            base + 'procurement-spend-over-25000-august-2018/?ind=1553337099854&filename=Procurement%20Spend%20Over%20%C2%A325,000,%20August%202018.csv&wpdmdl=22553&refresh=664f48e0bfd5d1716472032',
            base + 'procurement-spend-over-25000-july-2018/?ind=1553337131627&filename=Procurement%20Spend%20Over%2025000,%20July%202018.csv&wpdmdl=21037&refresh=664f48e19418e1716472033',
            base + 'procurement-spend-over-25000-june-2018/?ind=1553337404661&filename=Procurement%20Spend%20Over%2025000,%20June%202018.csv&wpdmdl=20440&refresh=664f48e2b15f11716472034',
            base + 'procurement-spend-over-25000-may-2018/?ind=1553337197271&filename=1553337197wpdm_Procurement%20Spend%20Over%2025000%20May%202018.csv&wpdmdl=19938&refresh=664f48e41337b1716472036',
            base + 'procurement-spend-over-25000-april-2018/?ind=1528281575410&filename=Procurement%20Spend%20Over%2025000%20April%202018.csv&wpdmdl=19677&refresh=664f48e4e4e011716472036',
            base + 'procurement-spend-over-25000-march-2018/?ind=1528281515879&filename=Copy%20of%20Procurement%20Spend%20Over%2025000%20March%202018.csv&wpdmdl=19678&refresh=664f48e67ca2b1716472038',
            base + 'procurement-spend-over-25000-february-2018/?ind=1525950305739&filename=Procurement%20Spend%20Over%2025000%20February%202018.csv&wpdmdl=19408&refresh=664f48e7aadac1716472039',
            base + 'procurement-spend-over-25000-january-2018/?ind=1524653424564&filename=Procurement%20Spend%20Over%20%C2%A325,000,%20January%202018.csv&wpdmdl=19305&refresh=664f48e87df1a1716472040',
            base + 'procurement-spend-over-25000-december-2017/?ind=1524653324167&filename=Procurement%20Spend%20Over%20%C2%A325,000,%20December%202017.csv&wpdmdl=19304&refresh=664f48e946ab91716472041',
            base + 'procurement-spend-25000-november-2017/?ind=1516194033992&filename=Procurement%20Spend%20Over%20%C2%A325,000,%20November%202017.csv&wpdmdl=18253&refresh=664f48ea5e3331716472042',
            base + 'procurement-spend-25000-october-2017/?ind=1516194007671&filename=Procurement%20Spend%20Over%20%C2%A325,000,%20October%202017.csv&wpdmdl=18250&refresh=664f48eb3723b1716472043',
            base + 'procurement-spend-25000-september-2017/?ind=1516193981922&filename=Procurement%20Spend%20Over%20%C2%A325,000,%20September%202017.csv&wpdmdl=18252&refresh=664f48ece33691716472044',
            base + 'procurement-spend-25000-august-2017/?ind=1516193952042&filename=Procurement%20Spend%20Over%20%C2%A325,000,%20August%202017.csv&wpdmdl=18246&refresh=664f48ee739ee1716472046',
            base + 'procurement-spend-25000-july-2017/?ind=1516193925793&filename=Procurement%20Spend%20Over%20%C2%A325,000,%20July%202017.csv&wpdmdl=18241&refresh=664f48ef59c5e1716472047',
            base + 'procurement-spend-25000-june-2017/?ind=1516193885661&filename=Procurement%20Spend%20Over%20%C2%A325,000,%20June%202017.csv&wpdmdl=18240&refresh=664f48f0264681716472048',
            base + 'procurement-spend-25000-may-2017/?ind=1516193854616&filename=Procurement%20Spend%20Over%20%C2%A325,000,%20May%202017.csv&wpdmdl=18237&refresh=664f48f11a3f81716472049',
            base + 'procurement-spend-25000-april-2017/?ind=1516193824674&filename=Procurement%20Spend%20Over%20%C2%A325,000,%20April%202017.csv&wpdmdl=18235&refresh=664f48f3586b01716472051',
            base + 'procurement-spend-25000-march-2017/?ind=1516193787200&filename=Procurement%20Spend%20Over%20%C2%A325,000,%20March%202017.csv&wpdmdl=18234&refresh=664f48f4714d81716472052',
            base + 'procurement-spend-25000-february-2017/?ind=1516193735457&filename=Procurement%20Spend%20Over%20%C2%A325,000,%20February%202017.csv&wpdmdl=18232&refresh=664f48f5a14291716472053',
            base + 'procurement-spend-25000-december-2016/?ind=1490099415408&filename=Procurement%20Spend%20Over%20%C2%A325,000,%20December%202016.csv&wpdmdl=15420&refresh=664f48f64db501716472054',
            base + 'procurement-spend-25000-november-2016/?ind=1490099376758&filename=Procurement%20Spend%20Over%20%C2%A325,000,%20November%202016.csv&wpdmdl=15417&refresh=664f48f7434761716472055',
            base + 'procurement-spend-over-25000-january-2017/?ind=1490099956781&filename=1490099862wpdm_Procurement%20Spend%20Over%20%C2%A325,000,%20January%202017.csv&wpdmdl=15405&refresh=664f48f8357a31716472056',
            base + 'procurement-spend-25000-october-2016/?ind=1482154686140&filename=Procurement%20Spend%20Over%20%C2%A325,000,%20October%202016.csv&wpdmdl=14511&refresh=664f48f9369ff1716472057',
            base + 'procurement-spend-25000-september-2016/?ind=1479743077608&filename=Procurement%20Spend%20Over%20%C2%A325,000,%20September%202016.csv&wpdmdl=14326&refresh=664f48fa84c121716472058',
            base + 'procurement-spend-25000-august-2016/?ind=1479742981611&filename=Procurement%20Spend%20Over%20%C2%A325,000,%20August%202016.csv&wpdmdl=14324&refresh=664f48fb3daea1716472059',
            base + 'procurement-spend-25000-july-2016/?ind=1479742890307&filename=Procurement%20Spend%20Over%20%C2%A325,000,%20July%202016.csv&wpdmdl=14323&refresh=664f48fc395a71716472060',
            base + 'procurement-spend-25000-june-2016/?ind=1469200276401&filename=Procurement%20Spend%20Over%20%C2%A325,000,%20June%202016.csv&wpdmdl=13383&refresh=664f48fe056ca1716472062',
            base + 'procurement-spend-25000-may-2016/?ind=1467718911465&filename=Procurement%20spend%20Over%20%C2%A325,000,%20May%202016.csv&wpdmdl=13147&refresh=664f48fecd9561716472062',
            base + 'procurement-spend-25000-april-2016/?ind=1467718872319&filename=Procurement%20spend%20Over%20%C2%A325,000,%20April%202016.csv&wpdmdl=13144&refresh=664f49004625c1716472064',
            base + 'procurement-spend-25000-march-2016/?ind=1467718828005&filename=Procurement%20spend%20Over%20%C2%A325,000,%20March%202016.csv&wpdmdl=13141&refresh=664f49015d7cd1716472065',
            base + 'procurement-spend-25000-february-2016/?ind=1467718778295&filename=Procurement%20spend%20Over%20%C2%A325,000,%20February%202016.csv&wpdmdl=13138&refresh=664f49023dc2e1716472066',
            base + 'procurement-spend-25000-january-2016/?ind=1467718718729&filename=Procurement%20Spend%20Over%20%C2%A325,000,%20January%202016.csv&wpdmdl=13135&refresh=664f4903232dc1716472067',
            base + 'procurement-spend-25000-december-2015/?ind=1467718667713&filename=Procurement%20spend%20Over%20%C2%A325,000,%20December%202015.csv&wpdmdl=13132&refresh=664f49041dd651716472068',
            base + 'procurement-spend-25000-november-2015/?ind=1467718594208&filename=Procurement%20Spend%20Over%20%C2%A325,000,%20November%202015.csv&wpdmdl=13129&refresh=664f4a2ea894b1716472366',
            base + 'procurement-spend-25000-october-2015/?ind=1467718527958&filename=Procurement%20Spend%20Over%20%C2%A325,000,%20October%202015.csv&wpdmdl=13126&refresh=664f4a2f8f69a1716472367',
            base + 'procurement-spend-over-25000-august-2015/?ind=0&filename=august%202015%20return.csv&wpdmdl=7018&refresh=664f4a30638c51716472368',
            base + 'procurement-spend-over-25000-september-2015/?ind=0&filename=1445512768wpdm_september%20return.csv&wpdmdl=6963&refresh=664f4a313353d1716472369',
            base + 'procurement-spend-over-25000-july-2015/?ind=0&filename=1445336112wpdm_july%20return.xlsx&wpdmdl=6825&refresh=664f4a32055561716472370',
            base + 'procurement-spend-over-25000-may-2015-2/?ind=0&filename=1445336000wpdm_june%20return.xlsx&wpdmdl=6819&refresh=664f4a32c936b1716472370',
            base + 'procurement-spend-over-25000-may-2015/?ind=0&filename=may%20return.csv&wpdmdl=5422&refresh=664f4a33854fd1716472371',
            base + 'procurement-spend-over-25000-april-2015/?ind=0&filename=april%202015%20return.csv&wpdmdl=5421&refresh=664f4a35e73501716472373',
            base + 'procurement-spend-over-25000-march-2015/?ind=0&filename=1437396453wpdm_march%20return.csv&wpdmdl=5420&refresh=664f4a36dbb5f1716472374',
            base + 'procurement-spend-over-25000-february-2015/?ind=0&filename=1437396380wpdm_february%20return.csv&wpdmdl=5419&refresh=664f4a37a917e1716472375',
            base + 'procurement-spend-over-25000-january-2015/?ind=0&filename=1437396206wpdm_january%20return.csv&wpdmdl=5418&refresh=664f4a38ee2761716472376',
            base + 'procurement-spend-25000-december-2014/?ind=0&filename=December%20return%202014.csv&wpdmdl=3733&refresh=664f4a39973961716472377',
            base + 'procurement-spend-25000-november-2014/?ind=0&filename=1421171714wpdm_November%20return%202014.csv&wpdmdl=3459&refresh=664f4a3aae1d91716472378',
            base + 'procurement-spend-25000-october-2014/?ind=0&filename=1419245765wpdm_October%20return%202014.csv&wpdmdl=3397&refresh=664f4a3c3a18f1716472380',
            base + 'procurement-spend-25000-september-2014-2/?ind=0&filename=1416912277wpdm_September%20return%202014.csv&wpdmdl=3290&refresh=664f4a3db18b31716472381',
            base + 'procurement-spend-25000-august-2014/?ind=0&filename=August%202014.xlsx&wpdmdl=3229&refresh=664f4a3f2f51e1716472383',
            base + 'procurement-spend-25000-july-2014/?ind=0&filename=July%202014.xlsx&wpdmdl=2193&refresh=664f4a4175ec71716472385',
            base + 'procurement-spend-25000-june-2014/?ind=0&filename=June%20return.xlsx&wpdmdl=2050&refresh=664f4a425a8941716472386',
            base + 'procurement-spend-25000-may-2014/?ind=0&filename=May%202014.csv&wpdmdl=1899&refresh=664f4a4336c061716472387',
            base + 'procurement-spend-25000-april-2014/?ind=0&filename=April%202014.csv&wpdmdl=1898&refresh=664f4a44d10a41716472388',
            base + 'procurement-spend-25000-march-2013-2/?ind=0&filename=1400153933wpdm_march%20return.csv&wpdmdl=1383&refresh=664f4a459e4be1716472389',
            base + 'procurement-spend-25000-february-2013-2/?ind=0&filename=1400153898wpdm_feb%20return.csv&wpdmdl=1382&refresh=664f4a46a51691716472390',
            base + 'procurement-spend-25000-january-2014/?ind=0&filename=january%20return.csv&wpdmdl=1381&refresh=664f4a47680321716472391',
            base + 'procurement-spend-25000-december-2013/?ind=0&filename=december%20return.csv&wpdmdl=1380&refresh=664f4a48643391716472392',
            base + 'procurement-spend-25000-november-2013/?ind=0&filename=november%20return.csv&wpdmdl=1379&refresh=664f4a4937f611716472393',
            base + 'procurement-spend-25000-october-2013/?ind=0&filename=october%20return.csv&wpdmdl=1378&refresh=664f4a4a37fb41716472394',
            base + 'procurement-spend-25000-september-2013/?ind=0&filename=september%20return.csv&wpdmdl=1377&refresh=664f4a4bafbc41716472395',
            base + 'procurement-spend-25000-august-2013/?ind=0&filename=august%20spend%2025000%20over.csv&wpdmdl=1376&refresh=664f4a4c894e91716472396',
            base + 'procurement-spend-25000-july-2013/?ind=0&filename=july%20return%20csv.csv&wpdmdl=1375&refresh=664f4a4d84ac61716472397',
            base + 'procurement-spend-25000-june-2013/?ind=0&filename=june%20returnv2%20csv.csv&wpdmdl=1374&refresh=664f4a4e5e2ba1716472398',
            base + 'procurement-spend-25000-may-2013/?ind=0&filename=may%202013%20return%20csv.csv&wpdmdl=1373&refresh=664f4a4f4f08f1716472399',
            base + 'procurement-spend-25000-april-2013/?ind=0&filename=april13%202013%20return%20csv.csv&wpdmdl=1372&refresh=664f4a5045c3b1716472400',
            base + 'procurement-spend-25000-march-2013/?ind=0&filename=march%202013%20report%20csv.csv&wpdmdl=1371&refresh=664f4a51018271716472401',
            base + 'procurement-spend-25000-february-2013/?ind=0&filename=sfo-spend-over-25k-february-2013.csv&wpdmdl=1370&refresh=664f4a5284c221716472402',
            base + 'procurement-spend-25000-january-2013/?ind=0&filename=sfo-spend-over-25k-january-2013.csv&wpdmdl=1369&refresh=664f4a53dfae81716472403',
            base + 'procurement-spend-25000-december-2012/?ind=0&filename=sfo-spend-over-25k-december-2012.csv&wpdmdl=1368&refresh=664f4a54ea2e31716472404',
            base + 'procurement-spend-25000-november-2012/?ind=0&filename=sfo-spend-over-25k-november-2012.csv&wpdmdl=1367&refresh=664f4a55c6a4b1716472405',
            base + 'procurement-spend-25000-october-2012/?ind=0&filename=sfo-spend-over-25k-october-2012.csv&wpdmdl=1366&refresh=664f4a56bd6751716472406',
            base + 'procurement-spend-25000-september-2012/?ind=0&filename=sfo-spend-over-25k-september-2012.csv&wpdmdl=1365&refresh=664f4a57b389d1716472407',
            base + 'procurement-spend-25000-august-2012/?ind=0&filename=sfo-spend-over-25k-august-2012.csv&wpdmdl=1364&refresh=664f4a58c58e31716472408',
            base + 'procurement-spend-25000-july-2012/?ind=0&filename=sfo-spend-over-25k-july-2012.csv&wpdmdl=1363&refresh=664f4a5a6ae5b1716472410',
            base + 'procurement-spend-25000-june-2012/?ind=0&filename=sfo-spend-over-25k-june-2012.csv&wpdmdl=1362&refresh=664f4a5b2a4801716472411',
            base + 'procurement-spend-25000-may-2012/?ind=0&filename=sfo-spend-over-25k-may-2012.csv&wpdmdl=1361&refresh=664f4a5cdd3b31716472412',
            base + 'procurement-spend-25000-april-2012/?ind=0&filename=sfo-spend-over-25k-april-2012.csv&wpdmdl=1360&refresh=664f4a5e954ab1716472414',
            base + 'procurement-spend-25000-october-2015/?ind=1467718527958&filename=Procurement%20Spend%20Over%20%C2%A325,000,%20October%202015.csv&wpdmdl=13126&refresh=664f4ecb6b6a61716473547',
        ]

        import urllib.parse
        def extract_filename_with_extension(text):
            decoded_text = urllib.parse.unquote(text)
            pattern = r'filename=([^&]+\.xlsx|[^&]+\.xls|[^&]+\.csv)'
            match = re.search(pattern, decoded_text)
            if match:
                return match.group(1)
            else:
                return ""

        save_directory = os.path.join(filepath, dept)
        for url in htmllist:
            filename = os.path.basename(url)
            filename = extract_filename_with_extension(filename)
            save_path = os.path.join(save_directory, filename)
            download_file(url, save_path)
    try:
        df = parse_data(filepath, dept, filestoskip=['Procurement Spend over 25,000 July 2019.xlsx'])
        df.to_csv(os.path.join(filepath, '..', '..', 'output',
                               'mergeddepts', dept + '.csv'), index=False)
    except Exception as e:
        module_logger.debug('CRITICAL problem: Cannot construct a merged '
                            'output dataframe for ' + dept)


def supcourt(filepath, dept):
    '''Notes: have to go via a third party website and create a custom function
    but otherwise seems to be working fine and well. Nicely grouped annually.
    How to update: check for existance of 2020.csv at:
        https://www.supremecourt.uk/about/transparency.html
    Most recent file: annual 2019 file -- specifies when most recently updated
    '''
    createdir(filepath, dept)
    if 'noscrape' not in sys.argv:
        SCbase = 'https://www.supremecourt.uk/docs/transparency-transactions-'

        htmllist = [
        #    SCbase + '2010.csv', SCbase + '2011.csv',
        #    SCbase + '2012.csv', SCbase + '2013.csv',
        #    SCbase + '2014.csv', SCbase + '2015.csv',
            SCbase + '2016.csv', SCbase + '2017.csv',
            SCbase + '2018.xlsx', SCbase + '2019.xlsx',
            SCbase + '2020.xlsx'
        ]
        for html_ in set(htmllist):
            try:
                if os.path.exists(os.path.join(filepath, dept,
                                               html_.split('/')[-1])) is False:
                    r = requests.get(html_)
                    with open(os.path.join(os.path.join(filepath, dept),
                                           html_.split('/')[-1]),
                              "wb") as csvfile:
                        csvfile.write(r.content)
                        module_logger.info('File downloaded: ' +
                                           ntpath.basename(html_))
            except Exception as e:
                module_logger.debug('Problem downloading ' +
                                    ntpath.basename(html_) +
                                    ': ' + str(e))
            time.sleep(1)
    try:
        df = parse_data(filepath, dept)
        df.to_csv(os.path.join(filepath, '..', '..', 'output',
                               'mergeddepts', dept + '.csv'), index=False)
    except Exception as e:
        module_logger.debug('CRITICAL problem: Cannot construct a merged '
                            'output dataframe for ' + dept)



def get_csv_links(url):
    try:
        # Send a GET request to the URL
        response = requests.get(url)
        response.raise_for_status()  # Check if the request was successful
    except requests.RequestException as e:
        print(f"Error fetching the URL: {e}")
        return []

    # Parse the content of the response with BeautifulSoup
    soup = BeautifulSoup(response.content, 'html.parser')

    # Find all <a> tags with href attributes
    links = soup.find_all('a', href=True)

    # Filter links that end with .csv
    csv_links = [link['href'] for link in links if link['href'].endswith('.csv')]

    # Resolve relative URLs to absolute URLs
    csv_links = [requests.compat.urljoin(url, link) for link in csv_links]

    return csv_links



def download_csv_files(csv_links, download_folder):
    for link in csv_links:
        try:
            file_name = os.path.join(download_folder,
                                     os.path.basename(link))
            with requests.get(link, stream=True) as r:
                r.raise_for_status()
                with open(file_name, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
            time.sleep(2)
        except requests.RequestException as e:
            print(f"Failed to download UKSTAT {link}: {e}")


def ukstatauth(filepath, dept):
    createdir(filepath, dept)


    url = "https://www.ons.gov.uk/aboutus/transparencyandgovernance/organisationdeclarations/paymentstosuppliersover25000"
    csv_links = get_csv_links(url)
    if csv_links:
        download_csv_files(csv_links, os.path.join(filepath, dept))

    try:
        df = parse_data(filepath, dept,
                        filestoskip=[])

        df.to_csv(os.path.join(filepath, '..', '..', 'output',
                               'mergeddepts', dept + '.csv'), index=False)
    except Exception as e:
        module_logger.debug('CRITICAL problem: Cannot construct a merged '
                            'output dataframe for ' + dept)

def ofwat(filepath, dept):
    ''' Note: complete listing on data.gov.uk. some deadlinks...
    How to update: automatic?: dataset/financial-transactions-data-ofwat
    Most recent file:December 2018
    '''
    createdir(filepath, dept)
    if 'noscrape' not in sys.argv:
        key = '43e2236d-f00a-4762-929d-2211e0ab5ad8/'
        landingpage = 'spend-over-25-000-in-ofwat'
        dataloc = [data + 'dataset/' + key + landingpage]
        get_data(dataloc, filepath, dept, exclusions=[
                 'prs_dat_transactions201107']
                 )
    try:
        df = parse_data(filepath, dept,
                        filestoskip=['prs_dat_transactions201008.csv',
                                     'prs_dat_transactions201208.csv',
                                     'prs_dat_transactions201110.csv',
                                     'prs_dat_transactions201206.csv',
                                     'prs_dat_transactions201501.csv',
                                     'prs_dat_transactions201202.csv',
                                     'prs_dat_transactions201410.csv',
                                     'prs_dat_transactions201503.csv',
                                     'prs_dat_transactions201412.csv'])
        df.to_csv(os.path.join(filepath, '..', '..', 'output',
                               'mergeddepts', dept + '.csv'), index=False)
    except Exception as e:
        module_logger.debug('CRITICAL problem: Cannot construct a merged '
                            'output dataframe for ' + dept)


def desnz(filepath, dept):
    ''' Note this changes from dclg in dec 2017.
    Note: therefore, grab all MHCLG only... this is a broken mess in general
    How to update: collections/mhclg-departmental-spending-over-250
    Most recent file: jan 2019
    '''
    createdir(filepath, dept)
    if 'noscrape' not in sys.argv:
        r = requests.get(
            base + 'collections/desnz-departmental-spending-over-25000')
        html_content = r.content
        soup = BeautifulSoup(html_content, 'html.parser')
        htmllist = []
        for a_tag in soup.find_all('a', href=True):
            href = a_tag['href']
            if href.startswith('/government/publications/desnz-spending'):
                htmllist.append('https://www.gov.uk' + href)
        htmllist = [x for x in htmllist if ("procurement" not in x) and
                    ("card" not in x) and ("card" not in x)]
        get_data(htmllist, filepath, dept)
    try:
        df = parse_data(filepath, dept,
                        filestoskip=[])

        df.to_csv(os.path.join(filepath, '..', '..', 'output',
                               'mergeddepts', dept + '.csv'), index=False)
    except Exception as e:
        module_logger.debug('CRITICAL problem: Cannot construct a merged '
                            'output dataframe for ' + dept)

def dsit(filepath, dept):
    ''' Note this changes from dclg in dec 2017.
    Note: therefore, grab all MHCLG only... this is a broken mess in general
    How to update: collections/mhclg-departmental-spending-over-250
    Most recent file: jan 2019
    '''
    createdir(filepath, dept)
    if 'noscrape' not in sys.argv:
        r = requests.get(
            base + 'collections/dsit-departmental-spending-over-25000')
        html_content = r.content
        soup = BeautifulSoup(html_content, 'html.parser')
        htmllist = []
        for a_tag in soup.find_all('a', href=True):
            href = a_tag['href']
            if href.startswith('/government/publications/dsit-spending'):
                htmllist.append('https://www.gov.uk' + href)
        htmllist = [x for x in htmllist if ("procurement" not in x) and
                    ("card" not in x) and ("card" not in x)]
        get_data(htmllist, filepath, dept)

    try:
        df = parse_data(filepath, dept)
        df.to_csv(os.path.join(filepath, '..', '..', 'output',
                               'mergeddepts', dept + '.csv'), index=False)
    except Exception as e:
        module_logger.debug('CRITICAL problem: Cannot construct a merged '
                            'output datafrme for ' + dept)
    try:
        df = parse_data(filepath, dept,
                        filestoskip=[])

        df.to_csv(os.path.join(filepath, '..', '..', 'output',
                               'mergeddepts', dept + '.csv'), index=False)
    except Exception as e:
        module_logger.debug('CRITICAL problem: Cannot construct a merged '
                            'output dataframe for ' + dept)


def build_merged(rawpath):
    ''' build merged databases'''
    print('\n>> Now working on Constructing Merged Departments!\n')
    filecountstart = sum([len(files) for r, d, files in os.walk(rawpath)])
    if 'depttype=nonministerial' not in sys.argv:
        modef(os.path.join(rawpath, 'ministerial'), 'modef')
        cabinetoffice(os.path.join(rawpath, 'ministerial'), 'cabinetoffice')
        dftransport(os.path.join(rawpath, 'ministerial'), 'dftransport')
        dohealth(os.path.join(rawpath, 'ministerial'), 'dohealth')
        dfeducation(os.path.join(rawpath, 'ministerial'), 'dfeducation')
        dfintdev(os.path.join(rawpath, 'ministerial'), 'dfintdev')
        dfinttrade(os.path.join(rawpath, 'ministerial'), 'dfinttrade')
        dworkpen(os.path.join(rawpath, 'ministerial'), 'dworkpen')
        mojust(os.path.join(rawpath, 'ministerial'), 'mojust')
        dcultmedsport(os.path.join(rawpath, 'ministerial'), 'dcultmedsport')
        ukexpfin(os.path.join(rawpath, 'ministerial'), 'ukexpfin')
        dbusenind(os.path.join(rawpath, 'ministerial'), 'dbusenind')
        dfeeu(os.path.join(rawpath, 'ministerial'), 'dfeeu')
        foroff(os.path.join(rawpath, 'ministerial'), 'foroff')
        hmtreas(os.path.join(rawpath, 'ministerial'), 'hmtreas')
        mhclg(os.path.join(rawpath, 'ministerial'), 'mhclg')
        nioff(os.path.join(rawpath, 'ministerial'), 'nioff')
        waleoff(os.path.join(rawpath, 'ministerial'), 'waleoff') # @TODO: These DL but dont parse, are all amounts negative!?
        scotoff(os.path.join(rawpath, 'ministerial'), 'scotoff')
        gldagohmcpsi(os.path.join(rawpath, 'ministerial'), 'gldagohmcpsi')
        homeoffice(os.path.join(rawpath, 'ministerial'), 'homeoffice')
        leaderlords()
        leadercommons()
        oags(os.path.join(rawpath, 'ministerial'), 'oags')
        defra(os.path.join(rawpath, 'ministerial'), 'defra')
        desnz(os.path.join(rawpath, 'ministerial'), 'desnz')
        dsit(os.path.join(rawpath, 'ministerial'), 'dsit')
    if 'depttype=ministerial' not in sys.argv:
        charcom(os.path.join(rawpath, 'nonministerial'), 'charcom')
        commarauth(os.path.join(rawpath, 'nonministerial'), 'commarauth')
        crownprosser(os.path.join(rawpath, 'nonministerial'), 'crownprosser')
        fsa(os.path.join(rawpath, 'nonministerial'), 'fsa')
        forcomm(os.path.join(rawpath, 'nonministerial'), 'forcomm')
        govlegdep()
        govaccdept(os.path.join(rawpath, 'nonministerial'), 'govaccdept')
        hmlandreg(os.path.join(rawpath, 'nonministerial'), 'hmlandreg')
        hmrc(os.path.join(rawpath, 'nonministerial'), 'hmrc')
        natsavinv(os.path.join(rawpath, 'nonministerial'), 'natsavinv')
        natarch(os.path.join(rawpath, 'nonministerial'), 'natarch')
        natcrimag()
        offrailroad(os.path.join(rawpath, 'nonministerial'), 'offrailroad')
        ofgem(os.path.join(rawpath, 'nonministerial'), 'ofgem')
        ofqual(os.path.join(rawpath, 'nonministerial'), 'ofqual')
        ofsted(os.path.join(rawpath, 'nonministerial'), 'ofsted')
        serfraud(os.path.join(rawpath, 'nonministerial'), 'serfraud')
        supcourt(os.path.join(rawpath, 'nonministerial'), 'supcourt') # @TODO This converts to HTML after 2020
        ukstatauth(os.path.join(rawpath, 'nonministerial'), 'ukstatauth')
        ofwat(os.path.join(rawpath, 'nonministerial'), 'ofwat') # @TODO Some files are redirecting, unclear whats going on here
    filecountend = sum([len(files) for r, d, files in os.walk(rawpath)])
    print('Added a total of ' + str(filecountend - filecountstart) +
          ' new files.')
