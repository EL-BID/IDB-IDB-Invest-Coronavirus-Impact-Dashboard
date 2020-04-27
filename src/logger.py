import pymsteams
import yaml
import traceback



def get_connection(path='configs/teams.yaml'):
    return pymsteams.connectorcard(yaml.load(open(path, 'r'))['channel'])

def post(content):
    
    con = get_connection()

    # create the section
    myMessageSection = pymsteams.cardsection()

    # Section Title
    myMessageSection.title("Waze TCP Pipeline Error")

    # Section Text
    myMessageSection.text('**ERROR:** ' + str(content))

    # Section Images
    myMessageSection.addImage("http://i.imgur.com/c4jt321l.png", ititle="This Is Fine")

    # Add your section to the connector card object before sending
    con.addSection(myMessageSection)
    
    con.summary("Test Message")

    con.send()