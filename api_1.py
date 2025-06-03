from flask import Flask, request, jsonify
import os

app = Flask(__name__)


# User class (similar to Go struct)
class User:
    def __init__(self, id, name, email):
        self.id = id
        self.name = name
        self.email = email

    def to_dict(self):
        return {"id": self.id, "name": self.name, "email": self.email}

    def __str__(self):
        return f"{self.id},{self.name},{self.email}"


# Global variable to store users (similar to Go's global slice)
users = []
file_name = "users_python.txt"  # Using a different filename to avoid conflict


# Function to load users from a file
def load_users_from_file():
    global users
    users = []  # Clear existing users before loading
    if os.path.exists(file_name):
        try:
            with open(file_name, "r") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        parts = line.split(",")
                        if len(parts) == 3:
                            try:
                                user_id = int(parts[0])
                                users.append(
                                    User(id=user_id, name=parts[1], email=parts[2])
                                )
                            except ValueError:
                                print(f"Skipping malformed line (ID not int): {line}")
                        else:
                            print(f"Skipping malformed line (not 3 parts): {line}")
        except IOError as e:
            print(f"Error opening or reading file: {e}")


# Function to save users to a file
def save_users_to_file():
    try:
        with open(file_name, "w") as f:
            for user in users:
                f.write(str(user) + "\n")
    except IOError as e:
        print(f"Error creating or writing to file: {e}")


# Load users at startup
load_users_from_file()


@app.route("/users", methods=["GET"])
def handle_get_users():
    # In Python, Flask handles method checking, but good practice to be explicit
    # if request.method != 'GET':
    #     return "Method Not Allowed", 405 # Flask handles this by default

    response_text = "Users:\n"
    for user in users:
        response_text += f"ID: {user.id}, Name: {user.name}, Email: {user.email}\n"
    return response_text, 200, {"Content-Type": "text/plain"}


@app.route(
    "/add", methods=["POST", "GET"]
)  # Allowing GET for browser testing like Go example
def handle_add_user():
    # The Go example uses query parameters for POST, which is unusual.
    # Typically, POST data comes in the request body.
    # Flask's request.args handles query parameters for both GET and POST.
    name = request.args.get("name")
    email = request.args.get("email")

    if not name or not email:
        return "Name and Email are required", 400

    # Simple ID generation
    new_id = len(users) + 1
    new_user = User(id=new_id, name=name, email=email)
    users.append(new_user)
    save_users_to_file()  # Saves every time

    return (
        f"User added: ID {new_user.id}, Name {new_user.name}, Email {new_user.email}\n",
        200,
        {"Content-Type": "text/plain"},
    )


if __name__ == "__main__":
    print("Server starting on http://localhost:8080")
    # Flask's development server is not recommended for production
    app.run(host="0.0.0.0", port=8081, debug=True)
