

def test_load_config_success():
    # This is the minimal expected YAML content as a string
    yaml_content = """
chamber:
  dev:
    VAULT_ROLE: "02f424c0-3b07-40cb-b6c0-73a732204e133"
    LOCKBOX_ID: "5c9969ea-0b05-467c-8dde-09995ea5c70f"
"""
    # Expected dictionary that mirrors the YAML structure
    expected_config = {
        'chamber': {
            'dev': {
                'VAULT_ROLE': '02f424c0-3b07-40cb-b6c0-73a732204e133',
                'LOCKBOX_ID': '5c9969ea-0b05-467c-8dde-09995ea5c70f'
            }
        }
    }

    # Mock the open function to simulate reading the above YAML content
    with patch("builtins.open", mock_open(read_data=yaml_content), create=True) as mocked_file:
        # Mock yaml.safe_load to directly return the expected configuration
        with patch("yaml.safe_load", return_value=expected_config) as mocked_yaml:
            # Execute the load_config function which we're testing
            config = config_read.load_config("dev")
            print("Loaded config:", config)  # Output the result for debugging

            # Assertions to check the function's effectiveness
            assert mocked_file.called, "File open not called"
            assert mocked_yaml.called, "YAML load not called"
            assert config is not None, "Configuration is None"
            assert 'chamber' in config, "Chamber config missing"
            assert 'dev' in config['chamber'], "Dev config under chamber missing"
            assert config['chamber']['dev']['VAULT_ROLE'] == '02f424c0-3b07-40cb-b6c0-73a732204e133', "VAULT_ROLE not matching"
            assert config['chamber']['dev']['LOCKBOX_ID'] == '5c9969ea-0b05-467c-8dde-09995ea5c70f', "LOCKBOX_ID not matching"
