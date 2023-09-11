import pytest
from unittest.mock import patch, Mock
import unittest
from pipeline.etl.extractdata import MarketData  

@pytest.fixture
def market_data():
    url = "https://tinyurl.com/jb-bank-m"
    output_folder = "test_output"
    return MarketData(url, output_folder)

def test_extract(market_data):
    with patch('urllib.request.urlretrieve', return_value=('path_to_zip', None)) as MockUrlRetrieve, \
         patch('zipfile.ZipFile') as MockZip:

        mock_zip = Mock()
        MockZip.return_value.__enter__.return_value = mock_zip

        result = market_data.extract()

        MockUrlRetrieve.assert_called_once_with(market_data.url)
        MockZip.assert_called_once_with('path_to_zip', 'r')
        mock_zip.extractall.assert_called_once_with(market_data.output_folder)
        assert result == mock_zip

def test_convert_asc_to_csv(market_data):
    with patch.object(MarketData, 'extract') as mock_extract, \
         patch('builtins.open', new_callable=unittest.mock.mock_open) as mock_open:



        mock_zip = Mock()
        mock_zip.namelist.return_value = ['file1.asc', 'file2.asc']
        mock_extract.return_value = mock_zip

        market_data.convert_asc_to_csv([])

        mock_extract.assert_called_once()
        assert mock_open.call_count == 4  
