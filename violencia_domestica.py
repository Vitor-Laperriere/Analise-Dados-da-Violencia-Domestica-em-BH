import pandas as pd
import sqlite3
import matplotlib.pyplot as plt


arquivos_ses = [
    'dados_violencia_mulheres_ses_2020.csv',
    'dados_violencia_mulheres_ses_2021.csv',
    'dados_violencia_mulheres_ses_2022.csv',
    'dados_violencia_mulheres_ses_2023.csv'
]

arquivos_policia = [
    'violencia_domestica_2020.csv',
    'violencia_domestica_2021.csv',
    'violencia_domestica_2022.csv',
    'violencia_domestica_2023.csv'
]

df_ses_list = [pd.read_csv(f, delimiter=';') for f in arquivos_ses]


df_policia_list = [pd.read_csv(f, delimiter=';') for f in arquivos_policia]

conn = sqlite3.connect('violencia_domestica.db')
cursor = conn.cursor()


create_table_ses = '''
CREATE TABLE IF NOT EXISTS dados_violencia_mulheres_ses (
    DT_NOTIFIC TEXT,
    DT_NASC TEXT,
    NU_IDADE_N TEXT,
    CS_SEXO TEXT,
    CS_RACA TEXT,
    ID_MN_RESI TEXT,
    LOCAL_OCOR TEXT,
    OUT_VEZES TEXT,
    LES_AUTOP TEXT,
    VIOL_FISIC TEXT,
    VIOL_PSICO TEXT,
    VIOL_SEXU TEXT,
    NUM_ENVOLV TEXT,
    AUTOR_SEXO TEXT,
    ORIENT_SEX TEXT,
    IDENT_GEN TEXT,
    ANO INTEGER
)
'''

create_table_policia = '''
CREATE TABLE IF NOT EXISTS violencia_domestica (
    municipio_cod TEXT,
    municipio_fato TEXT,
    data_fato TEXT,
    mes INTEGER,
    ano INTEGER,
    risp TEXT,
    rmbh TEXT,
    natureza_delito TEXT,
    tentado_consumado TEXT,
    qtde_vitimas INTEGER
)
'''


cursor.execute(create_table_ses)
cursor.execute(create_table_policia)

conn.commit()


def insert_data_ses(df, year):
    df['ANO'] = year
    for index, row in df.iterrows():
        cursor.execute('''
        INSERT INTO dados_violencia_mulheres_ses (
            DT_NOTIFIC, DT_NASC, NU_IDADE_N, CS_SEXO, CS_RACA, ID_MN_RESI, LOCAL_OCOR, OUT_VEZES,
            LES_AUTOP, VIOL_FISIC, VIOL_PSICO, VIOL_SEXU, NUM_ENVOLV, AUTOR_SEXO, ORIENT_SEX, IDENT_GEN, ANO
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', tuple(row))


def insert_data_policia(df):
    for index, row in df.iterrows():
        cursor.execute('''
        INSERT INTO violencia_domestica (
            municipio_cod, municipio_fato, data_fato, mes, ano, risp, rmbh, natureza_delito, tentado_consumado, qtde_vitimas
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', tuple(row))


for i, year in enumerate(range(2020, 2024)):
    insert_data_ses(df_ses_list[i], year)
    insert_data_policia(df_policia_list[i])


conn.commit()
conn.close()


def count_ocorrencias_pelo_ano(year):
    conn = sqlite3.connect('violencia_domestica.db')
    query_ses = f'''
    SELECT ID_MN_RESI as municipio, ANO, COUNT(*) as num_ocorrencias
    FROM dados_violencia_mulheres_ses
    WHERE ANO = {year}
    GROUP BY ID_MN_RESI, ANO
    ORDER BY num_ocorrencias DESC
    '''

    query_policia = f'''
    SELECT municipio_fato as municipio, ano, COUNT(*) as num_ocorrencias
    FROM violencia_domestica
    WHERE ano = {year}
    GROUP BY municipio_fato, ano
    ORDER BY num_ocorrencias DESC
    '''

    df_query_ses = pd.read_sql_query(query_ses, conn)
    df_query_policia = pd.read_sql_query(query_policia, conn)

    conn.close()
    return df_query_ses, df_query_policia


def get_top5_cities(year):
    df_query_ses, df_query_policia = count_ocorrencias_pelo_ano(year)
    top5_ses = df_query_ses.head(5)
    top5_policia = df_query_policia.head(5)
    return top5_ses, top5_policia


top5_results = []
for year in range(2020, 2024):
    top5_ses, top5_policia = get_top5_cities(year)
    top5_results.append((year, top5_ses, top5_policia))
    top5_ses.to_csv(f'top5_ses_{year}.csv', index=False)
    top5_policia.to_csv(f'top5_policia_{year}.csv', index=False)

def plot_top5_cities_comparison(year, top5_ses, top5_policia):
    plt.figure(figsize=(12, 6))
    width = 0.35
    indices = range(len(top5_ses))
    
    plt.bar(indices, top5_ses['num_ocorrencias'], width=width, label='SES', color='green')
    plt.bar([i + width for i in indices], top5_policia['num_ocorrencias'], width=width, label='Polícia', color='blue')
    
    plt.title(f'Comparação das 5 Cidades com Maior Número de Violência em {year}')
    plt.xlabel('Município')
    plt.ylabel('Número de Ocorrências')
    plt.xticks([i + width / 2 for i in indices], top5_ses['municipio'], rotation=45)
    plt.legend()
    plt.tight_layout()
    plt.grid(True)
    plt.show()


for year, top5_ses, top5_policia in top5_results:
    plot_top5_cities_comparison(year, top5_ses, top5_policia)

def count_ocorrencias_totais():
    conn = sqlite3.connect('violencia_domestica.db')
    query_ses = '''
    SELECT ANO, COUNT(*) as num_ocorrencias
    FROM dados_violencia_mulheres_ses
    GROUP BY ANO
    ORDER BY ANO
    '''

    query_policia = '''
    SELECT ano, COUNT(*) as num_ocorrencias
    FROM violencia_domestica
    GROUP BY ano
    ORDER BY ano
    '''

    df_ses = pd.read_sql_query(query_ses, conn)
    df_policia = pd.read_sql_query(query_policia, conn)

    conn.close()
    return df_ses, df_policia

df_ses, df_policia = count_ocorrencias_totais()

df_ocorrencias_totais = pd.merge(df_ses, df_policia, left_on='ANO', right_on='ano', suffixes=('_SES', '_Policia'))
df_ocorrencias_totais = df_ocorrencias_totais[['ANO', 'num_ocorrencias_SES', 'num_ocorrencias_Policia']]
df_ocorrencias_totais.to_csv('ocorrencias_totais_por_ano.csv', index=False)

def plot_tendencia(df_ocorrencias_totais):
    plt.figure(figsize=(12, 6))
    plt.plot(df_ocorrencias_totais['ANO'], df_ocorrencias_totais['num_ocorrencias_SES'], label='SES', color='green')
    plt.plot(df_ocorrencias_totais['ANO'], df_ocorrencias_totais['num_ocorrencias_Policia'], label='Polícia', color='blue')
    plt.title('Tendência de Ocorrências Totais ao Longo do Tempo')
    plt.xlabel('Ano')
    plt.ylabel('Número de Ocorrências')
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.show()

plot_tendencia(df_ocorrencias_totais)
