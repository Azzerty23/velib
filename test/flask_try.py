from flask import Flask, request, abort, redirect, url_for
app = Flask(__name__)
app.secret_key = '2d9-E2.)f&é,A$p@fpa+zSU03êû9_'

@app.route('/coucou/')
def dire_coucou():
    return 'Coucou !'

@app.route("/")
def index():
    print('Hello World!')
    return "Le chemin de 'racine' est : " + request.path

@app.route('/contact/')
def contact():
    mail = "jean@bon.fr"
    tel = "01 23 45 67 89"
    return "Mail: {} --- Tel: {}".format(mail, tel)

@app.route('/discussion/')
@app.route('/discussion/page/<int:num_page>')
def mon_chat(num_page=1):
    # num_page = int(num_page)
    premier_msg = 1 + 50 * (num_page - 1)
    dernier_msg = premier_msg + 50
    return 'affichage des messages {} à {}'.format(premier_msg, dernier_msg)

@app.errorhandler(500)
@app.errorhandler(404)
def ma_page_404(error):
    return "Ma jolie page {}".format(error.code), error.code

@app.route('/profil/')
def profil():
    if True:
        abort(401)
    return "Vous êtes bien identifié, voici la page demandée : ..."

@app.route('/google')
def redirection_google():
    return redirect('http://www.google.fr')

@app.route('/to_login/')
def to_login():
    if True:
        return redirect(url_for('page_de_login', pseudo='Toto'))
    return "Vous êtes bien identifié, voici la page demandée : ..."

@app.route('/login/<pseudo>')
def page_de_login(pseudo):
    return '+1'


if __name__ == "__main__":
    app.run(debug=True)
