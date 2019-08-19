from airflow import models, settings
from airflow.contrib.auth.backends.password_auth import PasswordUser

# Create Airflow User
user = PasswordUser(models.User())
user.username = "scoyne2"
user.email = "scoyne2@kent.edu"
user._set_password = "thisisatest"
session = settings.Session()
session.add(user)
session.commit()
session.close()
print("New User Created")
