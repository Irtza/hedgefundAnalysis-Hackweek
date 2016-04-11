from flask import Flask, render_template
from sqlalchemy import create_engine
import json
import plotly
import pandas as pd
import plotly.graph_objs as go

app = Flask(__name__)
#app.debug = True
app.config.from_pyfile('config.py')
#app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = True
#db = SQLAlchemy(app)

def read_postgrestable(readtablename):
    """
    returns : Pandas.Dataframe
    
    otherwise: returns False
    and prints exception
    
    """
    engine = create_engine('postgresql://irtza:hedgefund@localhost:5432/oquantdatabase')
    df = False
    try:
        df = pd.read_sql_query('select * from '+'"'+readtablename+'"',con=engine)
    except Exception, e:
        print str(e)
    return df

def savetoPostgres(df , table_name):
    '''
    Saves a DataFrame to a table in oquantdatabase PostgresSQL
    1st arg : DataFrame
    2nd arg : tablename in postgres .. will be created and overwritten
    
    Default: if exists = True
    '''
    engine = create_engine('postgresql://irtza:hedgefund@localhost:5432/oquantdatabase')
    try:
        #database table is also called bigdata
        pd.DataFrame.to_sql(df,table_name, engine,if_exists='replace')
        print "oquantdatabase table: "+table_name+": Over-written"

    except Exception ,e:
        print str(e)
        return False

    else:
        print "All the data has been BULK inserted to Postgresql: "
        return True

#Pandas Dataframe
df = read_postgrestable("HedgeFundResults")
million = 1000000.0

categories = {
    'less_250k' : df[df['tableValueTotal'].between(0, million/4)],
    'less_500k' : df[df['tableValueTotal'].between(million/4, million/2)],
    'less_m'    : df[df['tableValueTotal'].between(million/2 , million)],
    'less_5m'   : df[df['tableValueTotal'].between(million,5*million)],
    'less_10m'  : df[df['tableValueTotal'].between(5*million,10*million)],
    'less_100m' : df[df['tableValueTotal'].between(10*million,100*million)],
    'less_bn'   : df[df['tableValueTotal'].between(100*million , 1000*million)],
    'over_bn'   : df[df['tableValueTotal'] > 1000 * million ]
}

@app.route('/')
def index():
    
    # ToDo : 
    # Beautify HomePage after class
    return render_template('layouts/index.html')

@app.route('/0_250k.html')
def view1():
    graph = dict(
        data=[go.Bar(
            x=categories['less_250k']['Company Name'],
            y=categories['less_250k']['tableValueTotal']
        )],
        layout=dict(
            title='HedgeFunds managing 0k to 250k dollars',
            yaxis=dict(
                title="Value"
            ),
            xaxis=dict(
                title="Company Name"
            )
        )
    )
    graphJSON = json.dumps(graph, cls=plotly.utils.PlotlyJSONEncoder)
    return render_template('layouts/templatejsplot.html', graphJSON=graphJSON )


@app.route('/250k_500k.html')
def view2():
    graph = dict(
        data=[go.Bar(
            x=categories['less_500k']['Company Name'],
            y=categories['less_500k']['tableValueTotal']
        )],
        layout=dict(
            title='HedgeFunds managing 250k to 500k dollars',
            yaxis=dict(
                title="Value"
            ),
            xaxis=dict(
                title="Company Name"
            )
        )
    )
    graphJSON = json.dumps(graph, cls=plotly.utils.PlotlyJSONEncoder)
    return render_template('layouts/templatejsplot.html', graphJSON=graphJSON )


@app.route('/500k_m.html')
def view3():
    graph = dict(
        data=[go.Bar(
            x=categories['less_m']['Company Name'],
            y=categories['less_m']['tableValueTotal']
        )],
        layout=dict(
            title='HedgeFunds managing 500k to 1million dollars',
            yaxis=dict(
                title="Value"
            ),
            xaxis=dict(
                title="Company Name"
            )
        )
    )
    graphJSON = json.dumps(graph, cls=plotly.utils.PlotlyJSONEncoder)
    return render_template('layouts/templatejsplot.html', graphJSON=graphJSON )


@app.route('/1m_5m.html')
def view4():
    graph = dict(
        data=[go.Bar(
            x=categories['less_5m']['Company Name'],
            y=categories['less_5m']['tableValueTotal']
        )],
        layout=dict(
            title='HedgeFunds managing 1m to 5m dollars',
            yaxis=dict(
                title="Value"
            ),
            xaxis=dict(
                title="Company Name"
            )
        )
    )
    graphJSON = json.dumps(graph, cls=plotly.utils.PlotlyJSONEncoder)
    return render_template('layouts/templatejsplot.html', graphJSON=graphJSON )

@app.route('/5m_10m.html')
def view5():
    graph = dict(
        data=[go.Bar(
            x=categories['less_10m']['Company Name'],
            y=categories['less_10m']['tableValueTotal']
        )],
        layout=dict(
            title='HedgeFunds managing 1m to 10m dollars',
            yaxis=dict(
                title="Value"
            ),
            xaxis=dict(
                title="Company Name"
            )
        )
    )
    graphJSON = json.dumps(graph, cls=plotly.utils.PlotlyJSONEncoder)
    return render_template('layouts/templatejsplot.html', graphJSON=graphJSON )

@app.route('/10m_100m.html')
def view6():
    graph = dict(
        data=[go.Bar(
            x=categories['less_100m']['Company Name'],
            y=categories['less_100m']['tableValueTotal']
        )],
        layout=dict(
            title='HedgeFunds managing 10m to 100m dollars',
            yaxis=dict(
                title="Value"
            ),
            xaxis=dict(
                title="Company Name"
            )
        )
    )
    graphJSON = json.dumps(graph, cls=plotly.utils.PlotlyJSONEncoder)
    return render_template('layouts/templatejsplot.html', graphJSON=graphJSON )


@app.route('/100m_Bn.html')
def view7():
    graph = dict(
        data=[go.Bar(
            x=categories['less_bn']['Company Name'],
            y=categories['less_bn']['tableValueTotal']
        )],
        layout=dict(
            title='Hedge funds managing 100m to 1bn dollars',
            yaxis=dict(
                title="Value"
            ),
            xaxis=dict(
                title="Company Name"
            )
        )
    )
    graphJSON = json.dumps(graph, cls=plotly.utils.PlotlyJSONEncoder)
    return render_template('layouts/templatejsplot.html', graphJSON=graphJSON )

@app.route('/overaBillion.html')
def view8():

    # Create the Plotly Data Structure
    graph = dict(
        data=[go.Bar(
            x=categories['over_bn']['Company Name'],
            y=categories['over_bn']['tableValueTotal']
        )],
        layout=dict(
            title='Hedge funds managing over a Billion Dollars',
            yaxis=dict(
                title="Value"
            ),
            xaxis=dict(
                title="Company Name"
            )
        )
    )
    # Convert the figures to JSON
    graphJSON = json.dumps(graph, cls=plotly.utils.PlotlyJSONEncoder)
    # Render the Template
    return render_template('layouts/templatejsplot.html', graphJSON=graphJSON )
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=9999)
