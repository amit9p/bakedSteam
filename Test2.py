
import yaml

# Test YAML string -- simulating exactly what's in your config file
test_yaml = """
datasets:
  dataset_one:
    date_range:
      - [ "20180221", ]    # space before closing bracket
  dataset_two:
    date_range:
      - [ "20181001",  ]   # two spaces before closing bracket
  dataset_three:
    date_range:
      - ["20181001",]      # no space at all
"""

# Parse the YAML
config = yaml.safe_load(test_yaml)

# Print parsed results for each dataset
for dataset, values in config['datasets'].items():
    date_range = values['date_range'][0]  # get the list value
    start_date = date_range[0]            # first element -- start date
    end_date   = date_range[1]            # second element -- None/null
    
    print(f"{dataset} --> start: {start_date}, end: {end_date}")
