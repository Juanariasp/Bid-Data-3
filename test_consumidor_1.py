from consumidor_1 import check_bollinger

def test_empty_price_window():
    price = 190
    stock = "AAPL"
    price_window = []
    result = check_bollinger(price_window, price, stock)

    assert result is None

if __name__ == '__main__':
   test_empty_price_window()
