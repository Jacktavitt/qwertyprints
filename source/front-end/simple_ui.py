from app import app





# ====================================================== 
# app = Flask(__name__)

# # @app.route('/')
# # def hello_world():
# #     return "Hello world"

# @app.route('/user/<int:username>')
# def get_user_auth(username):
#     return '''<form method="POST">
#     <input name="text">
#     <input type="submit">
# </form>'''

# @app.route('/')
# def my_form():

#     return '''<textarea rows="4" cols="50"></textarea>'''


# @app.route('/', methods=['POST'])
# def my_form_post():
#     text = request.form['text']
#     processed_text = text.upper()
#     return processed_text

#     # '''<textarea rows="4" cols="50"></textarea>'''