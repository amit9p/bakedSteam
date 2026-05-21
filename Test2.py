
import yaml

test_yaml = """
datasets:
  dataset_one:
    date_range:
      - [ "20180221", ]
  dataset_two:
    date_range:
      - [ "20181001",  ]
  dataset_three:
    date_range:
      - ["20181001",]
"""

config = yaml.safe_load(test_yaml)

for dataset, values in config['datasets'].items():
    date_range = values['date_range'][0]  # get the inner list
    
    start_date = date_range[0]  # first element always exists
    
    # safely get end date -- use None if second element missing
    end_date = date_range[1] if len(date_range) > 1 else None
    
    print(f"{dataset} --> start: {start_date}, end: {end_date}")
