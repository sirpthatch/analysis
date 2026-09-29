def pytest_configure(config):
    config.addinivalue_line("markers", "network: hits the live Socrata APIs")
