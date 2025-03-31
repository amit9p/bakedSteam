
Here's a basic regex that can help you identify if a string looks like a US address:

^\d+\s+([A-Za-z0-9\s]+),\s*([A-Za-z\s]+),\s*(AL|AK|AZ|AR|CA|CO|CT|DE|FL|GA|HI|ID|IL|IN|IA|KS|KY|LA|ME|MD|MA|MI|MN|MS|MO|MT|NE|NV|NH|NJ|NM|NY|NC|ND|OH|OK|OR|PA|RI|SC|SD|TN|TX|UT|VT|VA|WA|WV|WI|WY)\s+\d{5}(-\d{4})?$


---

✅ Explanation:

^\d+ → Starts with a house/building number

([A-Za-z0-9\s]+) → Street name

([A-Za-z\s]+) → City name

(AL|AK|...|WY) → US State Code (mandatory)

\d{5}(-\d{4})? → ZIP code (5-digit or ZIP+4)
