
test_data = [
    (1, 1, 0, 0, 0, 1000, "None", "None", 0),  # PIF = 0
    (2, 0, 1, 0, 0, 1000, "None", "None", 0),  # SIF = 0
    (3, 0, 0, 1, 0, 1000, "None", "None", 0),  # Pre-CO SIF = 0
    (4, 0, 0, 0, 1, 1000, "None", "None", 0),  # Asset Sales = 0
    (5, 0, 0, 0, 0, 0, "None", "None", 0),  # Posted Balance <= 0
    (6, 0, 0, 0, 0, 1000, "Open", "13", 0),  # Open & Chapter 13 = 0
    (7, 0, 0, 0, 0, 1000, "Discharged", "None", 0),  # Discharged = 0
    (8, 0, 0, 0, 0, 800, "Dismissed", "None", 100),  # Dismissed => 800 - 100 = 700
    (9, 0, 0, 0, 0, 800, "Closed", "None", 100),  # Closed => 800 - 100 = 700
    (10, 0, 0, 0, 0, 800, None, None, 100),  # Null Bankruptcy Status => 800 - 100 = 700
    (11, 0, 0, 0, 0, 800, "Open", "07", 100),  # Open & Chapter 07 => 800 - 100 = 700
    (12, 0, 0, 0, 0, 800, "Open", "11", 100),  # Open & Chapter 11 => 800 - 100 = 700
    (13, 0, 0, 0, 0, 800, "Open", "None", 100),  # Open & No Chapter => 800 - 100 = 700
]


expected = [
    (1, 0),
    (2, 0),
    (3, 0),
    (4, 0),
    (5, 0),
    (6, 0),
    (7, 0),
    (8, 700),
    (9, 700),
    (10, 700),
    (11, 700),
    (12, 700),
    (13, 700),
]


