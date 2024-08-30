
database:
  dev:
    host: localhost
    port: 5432
    username: dev_user
    password: dev_password
  qa:
    host: qa.db.server
    port: 5432
    username: qa_user
    password: qa_password
  prod:
    host: prod.db.server
    port: 5432
    username: prod_user
    password: prod_password

api:
  dev:
    base_url: http://localhost:5000/api
    timeout: 30
  qa:
    base_url: http://qa.api.server/api
    timeout: 60
  prod:
    base_url: https://api.server.com/api
    timeout: 120

logging:
  dev:
    level: DEBUG
    filepath: /var/log/dev_app.log
  qa:
    level: INFO
    filepath: /var/log/qa_app.log
  prod:
    level: WARNING
    filepath: /var/log/prod_app.log
