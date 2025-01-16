import logging
import azure.functions as func
from tradingview_screener import Query, Column
import telebot
import datetime
from numerize import numerize 
import pandas as pd

app = func.FunctionApp()

@app.timer_trigger(schedule="0 52 3 * * 1-5", arg_name="myTimer", run_on_startup=False,
              use_monitor=False) 
def timer_trigger_india(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due!')
    
    logging.info("code started yey!!!")

    def number_to_crore(number):
        return f"{number / 10000000:.2f} Cr INR"
    
    def run1():
        df =(Query()
            #   .set_index('SYML:SP;SPX')
        .select('name', 'close', 'volume', 'RSI|5', 'RSI|30', "market_cap_basic")
        .set_markets('india')
        .where(
            Column('RSI|5')>60,
            Column('RSI|30')<50,
            Column('market_cap_basic')>100_000_000_000,
            Column("exchange").isin(["NSE"])
        )
        .order_by('market_cap_basic', ascending=False)
        .limit(500)
        .get_scanner_data())
        df[1]['market_cap'] = df[1]['market_cap_basic'].apply(number_to_crore)
        df[1]['RSI|5']=df[1]['RSI|5'].round(2)
        df[1]['RSI|30']=df[1]['RSI|30'].round(2)
        fin_val_1 = df[1].drop(columns=['market_cap_basic',"ticker"])
        return fin_val_1

    # def format(mc):
    #     return str(numerize.numerize(mc)) + " INR"

    def run2():
        df =(Query()
        .select('name', 'close', 'volume', 'RSI|5', 'RSI|30', "market_cap_basic")
        .set_markets('india')
        .where(
            Column('RSI|5')<40,
            Column('RSI|30')>50,
            Column('market_cap_basic')>100_000_000_000,
            Column("exchange").isin(["NSE"])
        )
        .order_by('market_cap_basic', ascending=False)
        .limit(500)
        .get_scanner_data())
        # def number_to_crore(number):
        #     return f"{number / 10000000:.2f} Cr INR"
        df[1]['market_cap'] = df[1]['market_cap_basic'].apply(number_to_crore)
        df[1]['RSI|5']=df[1]['RSI|5'].round(2)
        df[1]['RSI|30']=df[1]['RSI|30'].round(2)
        fin_val_2 = df[1].drop(columns=['market_cap_basic',"ticker"])
        return fin_val_2
    
    def create_centered_html_note(text, total_cols):
        note_row = [''] * total_cols
        middle_col = total_cols // 2
        note_row[middle_col] = text
        return note_row
    
    fin_val_1=run1()
    fin_val_2=run2()
    empty_df = pd.DataFrame([[""] * len(fin_val_1.columns)], columns=fin_val_1.columns)
    note_df_1 = pd.DataFrame([ ["RSI|5 > 60 and RSI|30 < 50"] + [''] * (len(fin_val_1.columns) - 1)], 
                          columns=fin_val_1.columns)
    note_df_2 = pd.DataFrame([["RSI|5 < 40 and RSI|30 > 50"] + [''] * (len(fin_val_1.columns) - 1)], 
                          columns=fin_val_1.columns)
    header = pd.DataFrame([fin_val_1.columns], columns=fin_val_1.columns)
    # Replace with your actual bot token and chat ID
    BOT_TOKEN = '7445418853:AAFhd5kICq-OZrjVhujMMBA7oNbQTdqtDwE' 
    CHAT_ID = '-1002382357094' 
    combined_df = pd.concat([empty_df, note_df_1, empty_df, header, fin_val_1, empty_df, note_df_2, empty_df, header , fin_val_2], ignore_index=True)
    bot = telebot.TeleBot(BOT_TOKEN)
    header1=['Name', 'Close', 'Volume', 'RSI|5', 'RSI|30', "Market_cap"]
    current_date = datetime.date.today()
    india_time = (datetime.datetime.now() + datetime.timedelta(hours=5,minutes=30)).strftime("%H:%M")
    csv=combined_df.to_csv("/tmp/india_data_"+str(current_date)+"_"+str(india_time)+".csv",index=False,sep=';',header=None)
    logging.info(combined_df)

    bot.send_document(
        chat_id=CHAT_ID, 
        document=open("/tmp/india_data_"+str(current_date)+"_"+str(india_time)+".csv", "rb"), 
    )

    # df[1]['market_cap'] = df[1]['market_cap_basic'].apply(format)
    # fin_val = df[1].drop(columns=['market_cap_basic',"ticker"])

    # # Replace with your actual bot token and chat ID
    # BOT_TOKEN = '7445418853:AAFhd5kICq-OZrjVhujMMBA7oNbQTdqtDwE' 
    # CHAT_ID = '-1002382357094' 

    # bot = telebot.TeleBot(BOT_TOKEN)
    # header1=['Name', 'Close', 'Volume', "Market_cap"]
    # current_date = datetime.date.today()
    # india_time = (datetime.datetime.now() + datetime.timedelta(hours=5,minutes=30)).strftime("%H:%M")
    # csv=fin_val.to_csv("/tmp/india_data_"+str(current_date)+"_"+str(india_time)+".csv",index=False,sep=';',header=header1)
    # logging.info(fin_val)

    # bot.send_document(
    #     chat_id=CHAT_ID, 
    #     document=open("/tmp/india_data_"+str(current_date)+"_"+str(india_time)+".csv", "rb"), 
    # )

    logging.info('Python timer trigger function executed.')