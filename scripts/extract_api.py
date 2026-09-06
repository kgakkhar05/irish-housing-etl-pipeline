import requests
url = "https://jsonplaceholder.typicode.com/posts"
response = requests.get(url)
print(response.status_code)
data = response.json()
print(type(data))