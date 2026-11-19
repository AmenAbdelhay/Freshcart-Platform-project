from airflow.utils.email import send_email

send_email(
    to=["amenabdelhay04@gmail.com"],
    subject="FreshCart - SMTP Test",
    html_content="<h2>Success!</h2><p>Airflow Gmail alert is working.</p>",
)

print("Email sent successfully!")