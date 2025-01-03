import logging
import azure.functions as func
from tradingview_screener import Query, Column
import telebot
import datetime
from numerize import numerize 

app = func.FunctionApp()

@app.timer_trigger(schedule="0 52 3 * * 1-5", arg_name="myTimer", run_on_startup=False,
              use_monitor=False) 
def timer_trigger_india(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due!')
    
    logging.info("code started yey!!!")
    
    df =(Query()
        #   .set_index('SYML:SP;SPX')
    .select('name', 'close', 'volume', "market_cap_basic")
    .set_markets('india')
    .where(
        Column('RSI')>70,
        Column('market_cap_basic')>100_000_000_000,
        Column("exchange").isin(["NSE"])
    )
    .order_by('market_cap_basic', ascending=False)
    .limit(500)
    .get_scanner_data())

    def format(mc):
        return str(numerize.numerize(mc)) + " INR"

    df[1]['market_cap'] = df[1]['market_cap_basic'].apply(format)
    fin_val = df[1].drop(columns=['market_cap_basic',"ticker"])

    # Replace with your actual bot token and chat ID
    BOT_TOKEN = '7445418853:AAFhd5kICq-OZrjVhujMMBA7oNbQTdqtDwE' 
    CHAT_ID = '-1002382357094' 

    bot = telebot.TeleBot(BOT_TOKEN)
    header1=['Name', 'Close', 'Volume', "Market_cap"]
    current_date = datetime.date.today()
    india_time = (datetime.datetime.now() + datetime.timedelta(hours=5,minutes=30)).strftime("%H:%M")
    csv=fin_val.to_csv("/tmp/india_data_"+str(current_date)+"_"+str(india_time)+".csv",index=False,sep=';',header=header1)
    logging.info(fin_val)

    bot.send_document(
        chat_id=CHAT_ID, 
        document=open("/tmp/india_data_"+str(current_date)+"_"+str(india_time)+".csv", "rb"), 
    )

    logging.info('Python timer trigger function executed.')