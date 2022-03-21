from __future__ import absolute_import
from unittest import result
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import zipfile
import os
import sys
from numpy import source
import requests
import re
import apache_beam as beam
from io import BytesIO
from pathlib import Path
import pandas as pd

# criar o diretório

# os.makedirs("./downloadteste", exist_ok=True)
os.makedirs("./dadosRF", exist_ok=True)

url_rf = "http://200.152.38.155/CNPJ/K3241.K03200Y0.D20212.EMPRECSV.zip"

# url_teste = "https://download.inep.gov.br/microdados/microdados_enem_2020.zip"

filebytes = BytesIO( requests.get(url_rf).content)

myzip = zipfile.ZipFile(filebytes)
# myzip.extractall("./downloadteste")
myzip.extractall("./dadosRF")

# O arquivo da receita levou 39 mins pra concluir

# Renomeia o arquivo trocando o nome e a extensão

p = Path("C:/Users/lfsto/Desktop/ROIT/RoitGCP/dadosRF/K3241.K03200Y0.D20212.EMPRECSV")
p.rename('cnpj_empresas.csv')

# Define os caminhos de entrada e saída

inputFile = "C:/Users/lfsto/Desktop/ROIT/RoitGCP/dadosRF/cnpj_empresas.csv"
outputFile = "C:/Users/lfsto/Desktop/ROIT/RoitGCP/dadosRF/dados_up"

# Instancia pipeline

options = PipelineOptions()

# Credenciais

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "C:/Users/lfsto/pygcp/GOOGLE_APPLICATION_CREDENTIALS.json"

# Definindo pipe options

class MyOptions(PipelineOptions):

    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--input',
                            help='C:/Users/lfsto/Desktop/ROIT/RoitGCP/dadosRF/cnpj_empresas.csv',
                            dest='input',
                            required='False',
                            default='')
        parser.add_argument('--output',
                            help='Output for the pipeline',
                            required='False',
                            default='')

# classe para dividir as linhas do csv por elementos e retornar apenas as colunas que estamos interessados

class Split(beam.DoFn):
    def process(self, element):
        cnpjBasico, razSocial_nomEmpresarial, natJuridica, qualResponsavel, capSocial, porteEmpresa = element.split(";")
        return [{
            'cnpjBasico': int(cnpjBasico),
            'razSocial_nomEmpresarial': str(razSocial_nomEmpresarial),
            'natJuridica': int(natJuridica),
            'qualResposavel': int(qualResponsavel),
            'capSocial': int(capSocial),
            'porteEmpresa': int(porteEmpresa)
        }]

# data = ('./cnpj_empresas.csv')

cols = ['cnpjBasico', 'razSocial_nomEmpresarial', 'natjuridica', 'qualResponsavel', 'capSocial', 'porteEmpresa', 'enteFed']

# Carregamento de dados

data_raw = pd.read_csv('C:/Users/lfsto/Desktop/ROIT/teste_pratico/pipe/cnpjpubli1.csv', encoding='cp1252', sep=';', header=None, names=cols, decimal=',', low_memory=False)

#remove numeros da coluna  razSocial_nomEmpresarial

data_raw['razSocial_nomEmpresarial'] = data_raw['razSocial_nomEmpresarial'].str.replace('\d+', '') # Remove números da coluna

data_raw['porteEmpresa'].fillna(0)

# Excluir coluna enteFed

dropFed = data_raw['enteFed']
data_raw = data_raw(dropFed, axis=1)

df_prePronto = pd.DataFrame(data_raw)

df_prePronto.to_csv('outputFile')

from google.cloud import storage

# upload csv

def upload():

    storage_client = storage.Clent()
    bucket = storage_client.bucket()
    blob = bucket.blob(destination)

    blob.upload(source)