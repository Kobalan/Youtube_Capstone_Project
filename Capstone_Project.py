#1)Libraries
import streamlit as st
import mysql.connector
import pandas as pd
import plotly.express as px
from googleapiclient.discovery import build


#---------------------------------------------------------------------------
#2) Page Configuration
st.set_page_config(page_title="Capstone_Project",
                   layout= "wide",
                   initial_sidebar_state= "expanded")
                   
                   
st.sidebar.markdown("# :black[🅲🅰🅿🆂🆃🅾🅽🅴_🅿🆁🅾🅹🅴🅲🆃]")
st.sidebar.image('Youtube_logo.jpg')

st.sidebar.markdown(" :blue[🅰🅱🅾🆄🆃 🆃🅷🅸🆂 🅿🆁🅾🅹🅴🅲🆃]")
st.sidebar.markdown("##  :violet[█▓▒▒░░░PROJECT_TITILE░░░▒▒▓█]")
st.sidebar.markdown(" 𝒀𝒐𝒖𝒕𝒖𝒃𝒆 𝑯𝒂𝒓𝒗𝒆𝒔𝒕𝒊𝒏𝒈 𝒂𝒏𝒅 𝑾𝒂𝒓𝒆𝒉𝒐𝒖𝒔𝒊𝒏𝒈 𝑼𝒔𝒊𝒏𝒈 𝑺𝑸𝑳 𝒂𝒏𝒅 𝑺𝒕𝒓𝒆𝒂𝒎𝒍𝒊𝒕")
st.sidebar.markdown("## :violet[█▓▒▒░░░PROGRAMMING_LANGUAGE░░░▒▒▓█]")
st.sidebar.markdown("𝒑𝒚𝒕𝒉𝒐𝒏")
st.sidebar.markdown("## :violet[█▓▒▒░░░LIBRARIES_USED░░░▒▒▓█]")
st.sidebar.markdown("𝑷𝒂𝒏𝒅𝒂𝒔, 𝑮𝒐𝒐𝒈𝒍𝒆_𝑨𝑷𝑰, 𝑴𝒚𝒔𝒒𝒍, 𝑺𝒕𝒓𝒆𝒂𝒎𝒍𝒊𝒕") 
st.sidebar.markdown("## :violet[█▓▒▒░░░DOMAIN░░░▒▒▓█]")
st.sidebar.markdown("𝑺𝒐𝒄𝒊𝒂𝒍 𝑴𝒆𝒅𝒊𝒂")
st.sidebar.markdown ("## :violet[█▓▒▒░░░OVERVIEW░░░▒▒▓█]")
st.sidebar.markdown('𝑻𝒉𝒊𝒔 𝑷𝒓𝒐𝒋𝒆𝒄𝒕 𝒊𝒏𝒗𝒐𝒍𝒗𝒆𝒔 𝒃𝒖𝒊𝒍𝒅𝒊𝒏𝒈 𝒂 𝒔𝒊𝒎𝒑𝒍𝒆 𝑼𝑰 𝒘𝒊𝒕𝒉 𝑺𝒕𝒓𝒆𝒂𝒎𝒍𝒊𝒕, 𝒓𝒆𝒕𝒓𝒊𝒆𝒗𝒊𝒏𝒈 𝒅𝒂𝒕𝒂 𝒇𝒓𝒐𝒎 𝒕𝒉𝒆 𝒀𝒐𝒖𝑻𝒖𝒃𝒆 𝑨𝑷𝑰, 𝒔𝒕𝒐𝒓𝒊𝒏𝒈 𝒕𝒉𝒆 𝒅𝒂𝒕𝒂 𝑺𝑸𝑳 𝒂𝒔 𝒂 𝒘𝒂𝒓𝒆𝒉𝒐𝒖𝒔𝒆, 𝒒𝒖𝒆𝒓𝒚𝒊𝒏𝒈 𝒕𝒉𝒆 𝒅𝒂𝒕𝒂 𝒘𝒂𝒓𝒆𝒉𝒐𝒖𝒔𝒆 𝒘𝒊𝒕𝒉 𝑺𝑸𝑳, 𝒂𝒏𝒅 𝒅𝒊𝒔𝒑𝒍𝒂𝒚𝒊𝒏𝒈 𝒕𝒉𝒆 𝒅𝒂𝒕𝒂 𝒊𝒏 𝒕𝒉𝒆 𝑺𝒕𝒓𝒆𝒂𝒎𝒍𝒊𝒕 𝒂𝒑𝒑')
st.sidebar.markdown('## :violet[█▓▒▒░░░AUTHOR░░░▒▒▓█]')
st.sidebar.markdown("# :orange[🅼. 🅺🅾🅱🅰🅻🅰🅽]")

#------------------------------------------------------------------------------

#3)Database Connection


##Creating or Connecting a Database in SQL....

database= mysql.connector.connect(host="localhost",user ="root",
  password ="kobalan",auth_plugin="mysql_native_password",database="youtube")
cursor=database.cursor()
# cursor.execute("CREATE DATABASE youtube")

#-----------------------------------------------------------------------------
    
#4)Data Retrieval using Functions

#API_Connection...

api_key = 'AIzaSyBMJHpVbZCMo65P3qucFfKM9nhYx4_h67A'  #Google_API_Key
youtube = build("youtube", "v3", developerKey=api_key)
#Storing function in variable for reusable    


#1]Getting Channel_Details using Channel_id......

def get_channelDetails(id):
    channel_id = id  # Input Channel_ID.......
   
#Getting Channel_Details using Channel_ID.......
    request = youtube.channels().list(
        id=channel_id,
        part='snippet,statistics,contentDetails'
        )
    
    response = request.execute()                            #get this details in Google API Reference
    for i in response['items']:
        data=dict(
                Channel_id=channel_id,
                Channel_Name=i['snippet']['title'],
                Channel_description=i['snippet']['description'],
                Subscription_Count=i['statistics']['subscriberCount'],
                Channel_Views=i['statistics']['viewCount'],
                Total_Video_Count=i['statistics']['videoCount'],
                Playlist_Id=i['contentDetails']['relatedPlaylists']['uploads']
              )
    return data

#------------------------------------------------------------------------------

#2]Getting Video IDs for retrieving Video_Details.......

def get_videoID(Channel_id):
    video_ids=[]
    request1 = youtube.channels().list(
            id=Channel_id,
            part='contentDetails')
    response1=request1.execute()
    #For getting Video IDs we need Channel_Playlist ID
    playlist_ID=response1['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token=None
    while True:
              request2=youtube.playlistItems().list(
                      part='snippet',
                      playlistId=playlist_ID,
                      maxResults=50,
                      pageToken=next_page_token
                  )
              response2=request2.execute()
              for i in range(len(response2['items'])):
                  video_ids.append(response2['items'][i]['snippet']['resourceId']['videoId'])
              next_page_token=response2.get('nextPageToken')
              if next_page_token is None:
                break    
    return video_ids

#------------------------------------------------------------------------------
#3]Getting Video Details using Video_ID.....

def get_videoDetails(video_IDs):
    video_data=[]

    for video_id in video_IDs:
        request=youtube.videos().list(
            part="snippet,ContentDetails,statistics",
            id=video_id
        )
        
        response=request.execute()
        for item in response['items']:
                data=dict(
                    Channel_name = item['snippet']['channelTitle'],
                    Channel_id = item['snippet']['channelId'],
                    Video_ID=item['id'],
                    Video_name=item['snippet']['title'],
                    Tags=item['snippet'].get('tags'),        
                    Published_Date=item['snippet']['publishedAt'],                        
                    Views_Count=item['statistics']['viewCount'],
                    Likes_Count=item['statistics'].get('likeCount'),                                               
                    Favorite_Count=item['statistics']['favoriteCount'], 
                    Comment_Count=item['statistics']['commentCount'],
                    Duration=item['contentDetails']['duration'],
                    Thumbnail=item['snippet']['thumbnails'],
                    Caption_Status=item['contentDetails']['caption'],
                    Description=item['snippet']['description']
                    )
        video_data.append(data)
    return video_data
#-----------------------------------------------------------------------------
#4]Getting Comment_Details using Video_ID....

def get_commentDetails(video_IDs):
    try:
        comment_data=[]
        for video_id in video_IDs:
            request=youtube.commentThreads().list(
                        part="snippet",
                        videoId=video_id
            )        
            response=request.execute()
            for item in response['items']:
                data=dict(
                        video_ID=item['snippet']['topLevelComment']['snippet']['videoId'],
                        Comment_ID=item['id'],
                        Comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                        Comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        Comment_PublishedAt=item['snippet']['topLevelComment']['snippet']['publishedAt']
                        )
                comment_data.append(data)  
    except:
        pass              
    return comment_data


#------------------------------------------------------------------------------        
#5]Creating Channel Table in SQL.... 
def  channel_Table(channel_ID):
    ch_List=[]
    # Dropping the table if already created..
    # drop_query='''drop table if exists Channels'''
    # cursor.execute(drop_query)
    # database.commit()

    #Creating Table for Channel_Details
    Channel_details = """CREATE TABLE  IF NOT EXISTS Channels(
                        Channel_name  VARCHAR(100),
                        Channel_id VARCHAR(50) primary key,
                        Subscribers INT ,
                        Views int,
                        Total_Videos int,
                        Channel_Description text,
                        Playlist_Id varchar(100)
                        )"""
    #  # SQL_table created
    cursor.execute(Channel_details)
    database.commit()

#Inserting values into SQL_table from dataFrame...
    
    channel_details=get_channelDetails(channel_ID)
    ch_List.append(channel_details)
    df_Channel_details=pd.DataFrame(ch_List)
    for index,row in df_Channel_details.iterrows():
        insert_values='''insert into Channels(Channel_name,
                        Channel_id,
                        Subscribers,
                        Views,
                        Total_Videos,
                        Channel_Description,
                        Playlist_Id)

                        values(%s,%s,%s,%s,%s,%s,%s)'''
        
        values=(row['Channel_Name'],
                row['Channel_id'],
                row['Subscription_Count'],
                row['Channel_Views'],
                row['Total_Video_Count'],
                row['Channel_description'],
                row['Playlist_Id'])
        cursor.execute(insert_values,values)
        database.commit()
    
#-----------------------------------------------------------------------------
#6]Creating Video Table in SQL.... 
def video_Table(channel_ID):                                                                                                                                                                                                                                                                         
    vi_List=[]
    #Dropping the table if already created..
    # drop_query='''drop table if exists Videos'''
    # cursor.execute(drop_query)
    # database.commit()
    
    #Creating Table for Video_Details
    #Tags text,Thumbnail varchar(200),
    Video_details = """CREATE TABLE  IF NOT EXISTS Videos(
                        Channel_id varchar(30),
                        Channel_name varchar(50),
                        video_ID varchar(30) primary key,
                        Video_name varchar(150),
                        Description text,                      
                        Published_Date text,                        
                        Views_Count bigint,
                        Likes_Count bigint,    
                        Favorite_Count int, 
                        Comment_Count int,
                        Duration text,
                        
                        Caption_Status varchar(50)
                        )"""
    #  # SQL_table created
    cursor.execute(Video_details)
    database.commit()

    video_IDs=get_videoID(channel_ID)
    video_Details=get_videoDetails(video_IDs)
    for i in range(len(video_IDs)):
        vi_List.append(video_Details[i])
    df_Video_details=pd.DataFrame(vi_List)
   
    #Inserting values into SQL_table from dataFrame...
    for index,row in df_Video_details.iterrows():
        insert_values='''insert into videos (
                        Channel_id,
                        Channel_name,                         
                        video_ID,
                        Video_name,
                        Description,                 
                        Published_Date,                        
                        Views_Count,
                        Likes_Count,    
                        Favorite_Count, 
                        Comment_Count,
                        Duration,
                        Caption_Status)
                        values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        
        values=(row['Channel_id'],
                row['Channel_name'],
                row['Video_ID'],
                row['Video_name'],
                row['Description'],
                row['Published_Date'],
                row['Views_Count'],
                row['Likes_Count'],
                row['Favorite_Count'],
                row['Comment_Count'],
                row['Duration'],
                row['Caption_Status']
                )       
        cursor.execute(insert_values,values)
        database.commit()

#-----------------------------------------------------------------------------
#6]Creating Comment Table in SQL....       
def comment_Table(channel_ID):
    cmt_List=[]
    #Dropping the table if already created..
    # drop_query='''drop table if exists Comments'''
    # cursor.execute(drop_query)
    # database.commit()
#Creating Table for Comment_Details

    Comment_details = """CREATE TABLE  IF NOT EXISTS Comments(
                    video_ID  VARCHAR(100),
                    Comment_ID VARCHAR(50) primary key,
                    Comment_Text text,
                    Comment_Author varchar(100),
                    Comment_PublishedAt text
                    
                    )"""
    #  # SQL_table created
    cursor.execute(Comment_details)
    database.commit()
  
    video_IDs=get_videoID(channel_ID)
    comment_Details=get_commentDetails(video_IDs)
    for i in range(len(comment_Details)):
        cmt_List.append(comment_Details[i])
    df_Comment_details=pd.DataFrame(cmt_List)

    #Inserting values into SQL_table from dataFrame...
    #Comment_PublishedAt
    for index,row in df_Comment_details.iterrows():
        insert_values='''insert into Comments(
                    video_ID,
                    Comment_ID,
                    Comment_Text,
                    Comment_Author,
                    Comment_PublishedAt
                    )
                        values(%s,%s,%s,%s,%s)'''
        
        values=(row['video_ID'],
                row['Comment_ID'],
                row['Comment_Text'],
                row['Comment_Author'],
                row['Comment_PublishedAt']
        )
        cursor.execute(insert_values,tuple(values))
        database.commit()
    
#-----------------------------------------------------------------------------
#7] Creating functions for Showing table data
def Channel_opt():
    cursor.execute("""SELECT * FROM channels""")
    df = pd.DataFrame(cursor.fetchall())
    return st.table(df)
def Video_opt():
    cursor.execute("""SELECT * FROM videos""")
    df = pd.DataFrame(cursor.fetchall())
    return st.table(df)
def Comment_opt():
    cursor.execute("""SELECT * FROM comments""")
    df = pd.DataFrame(cursor.fetchall())
    return st.table(df) 
     
#-------------------------------------------------------------------------------
#5)SQL Transformation....

st.markdown("## :orange[  █▓▒TRANSFORMING▒▓]")
st.markdown("### 𝙋𝙖𝙨𝙩𝙚 𝙖 𝘾𝙝𝙖𝙣𝙣𝙚𝙡_𝙄𝘿")
channel_ID=st.text_input('𝙄𝘿...')
st.markdown("𝑯𝑰𝑵𝑻: 𝑽𝒊𝒔𝒊𝒕( 𝒘𝒘𝒘.𝒚𝒐𝒖𝒕𝒖𝒃𝒆.𝒄𝒐𝒎 )--> 𝑮𝑶𝑻𝑶 𝑪𝒉𝒂𝒏𝒏𝒆𝒍 '𝒔 𝑷𝒂𝒈𝒆--> 𝑨𝑩𝑶𝑼𝑻--> 𝑺𝑯𝑨𝑹𝑬 𝑪𝒉𝒂𝒏𝒏𝒆𝒍_𝑩𝒖𝒕𝒕𝒐𝒏 -->  𝑪𝑳𝑰𝑪𝑲 𝑪𝒐𝒑𝒚 𝑪𝒉𝒂𝒏𝒏𝒆𝒍_𝑰𝑫")
button=st.button(":blue[𝑻𝒓𝒂𝒏𝒔𝒇𝒐𝒓𝒎]")
ch_IDs=[]
if button:
    with st.spinner('Please Wait for it...'):
        cursor.execute("""SELECT Channel_id FROM channels""")
        df = pd.DataFrame(cursor.fetchall())
        for i in range(len(df)):
            ch_IDs.append(df[0][i])  
        if len(channel_ID)==0:    
             st.info("Please Enter Channel_ID")    
        elif channel_ID in ch_IDs:
            st.error("Already Inserted")
        else:  
                      
            channel_Table(channel_ID)
            video_Table(channel_ID)
            comment_Table(channel_ID)
            st.success("Transformed to MySQL Successfully!!!")


#6)Viewing Tables
    
st.markdown("## :orange[█▓▒▒VIEWING▒▒▓█]")
st.markdown("### 𝙎𝙚𝙡𝙚𝙘𝙩 𝙖 𝙏𝙖𝙗𝙡𝙚 𝙩𝙤 𝙎𝙝𝙤𝙬:")
res_View=st.selectbox("",options=('Click the table you want to see','Channel_Table','Video_Table','Comment_Table'))
button2=st.button(":blue[𝑺𝒉𝒐𝒘]")
if button2:
    if res_View=='Channel_Table':
        Channel_opt()
    elif res_View=='Video_Table':
        Video_opt()   
    elif res_View=='Comment_Table':
        Comment_opt()
#-------------------------------------------------------------------------
 
#7)Query Page


st.markdown("## :orange[█▓▒▒FREQUENTLY ASKED QUESTIONS▒▒▓█]")   
st.markdown("###  𝘾𝙝𝙤𝙤𝙨𝙚 𝙊𝙣𝙚:")


questions = st.selectbox('',
['Click the question that you would like to query',
'1. What are the names of all the videos and their corresponding channels?',
'2. Which channels have the most number of videos, and how many videos do they have?',
'3. What are the top 10 most viewed videos and their respective channels?',
'4. How many comments were made on each video, and what are their corresponding video names?',
'5. Which videos have the highest number of likes, and what are their corresponding channel names?',
'6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
'7. What is the total number of views for each channel, and what are their corresponding channel names?',
'8. What are the names of all the channels that have published videos in the year 2022?',
'9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
'10. Which videos have the highest number of comments, and what are their corresponding channel names?'])
        

if questions == '1. What are the names of all the videos and their corresponding channels?':
    cursor.execute("""SELECT Video_name AS Video_Title, Channel_name AS Channel_Name FROM videos ORDER BY Channel_name""")
    df = pd.DataFrame(cursor.fetchall(),columns=cursor.column_names)
    st.write(df)

elif questions == '2. Which channels have the most number of videos, and how many videos do they have?':
    cursor.execute("""SELECT Channel_name,Total_videos 
                        FROM channels
                        ORDER BY Total_videos DESC""")
    df = pd.DataFrame(cursor.fetchall(),columns=cursor.column_names)
    st.write(df)
    fig = px.bar(df,
                    x=cursor.column_names[0],
                    y=cursor.column_names[1],
                    orientation='v',
                    color=cursor.column_names[0]
                )
    
    st.plotly_chart(fig,use_container_width=True)
    

elif questions == '3. What are the top 10 most viewed videos and their respective channels?':
    cursor.execute("""SELECT  Channel_Name,Video_Name,Views_Count
                        FROM videos
                        ORDER BY Views_Count DESC
                        LIMIT 10""")
    df = pd.DataFrame(cursor.fetchall(),columns=cursor.column_names)
    st.write(df)
   
    fig = px.bar(df,
                    x=cursor.column_names[2],
                    y=cursor.column_names[1],
                    orientation='h',
                    color=cursor.column_names[0]
                )
    st.plotly_chart(fig,use_container_width=True)  

elif questions == '4. How many comments were made on each video, and what are their corresponding video names?':
    cursor.execute("""SELECT video_ID,Channel_name,Video_name,Comment_Count
                        FROM videos 
                        """)
    df = pd.DataFrame(cursor.fetchall(),columns=cursor.column_names)
    st.write(df)
  

          
elif questions == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
    cursor.execute("""SELECT Channel_Name,Video_name,Likes_Count 
                        FROM videos
                        ORDER BY Likes_Count DESC
                        LIMIT 10""")
    df = pd.DataFrame(cursor.fetchall(),columns=cursor.column_names)
    st.write(df)
    fig = px.bar(df,
                    x=cursor.column_names[2],
                    y=cursor.column_names[1],
                    orientation='h',
                    color=cursor.column_names[0]
                )
    st.plotly_chart(fig,use_container_width=True)


elif questions == '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
        cursor.execute("""SELECT Video_name,Likes_Count
                            FROM videos
                            ORDER BY Likes_Count DESC""")
        df = pd.DataFrame(cursor.fetchall(),columns=cursor.column_names)
        st.write(df)

elif questions == '7. What is the total number of views for each channel, and what are their corresponding channel names?':
        cursor.execute("""SELECT Channel_Name,Views
                            FROM channels
                            ORDER BY Views DESC""")
        df = pd.DataFrame(cursor.fetchall(),columns=cursor.column_names)
        st.write(df)

        fig = px.bar(df,
                     x=cursor.column_names[0],
                     y=cursor.column_names[1],
                     orientation='v',
                     color=cursor.column_names[0]
                    )
        fig.update_traces(textfont_size=12, textangle=0, textposition="outside", cliponaxis=False)
        st.plotly_chart(fig,use_container_width=True)


elif questions == '8. What are the names of all the channels that have published videos in the year 2022?':
        cursor.execute("""SELECT channel_name,Video_name,published_Date FROM videos WHERE extract(year from published_date)=2022""")
        df = pd.DataFrame(cursor.fetchall(),columns=cursor.column_names)
        st.write(df)

elif questions == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
        cursor.execute("""SELECT channel_name, 
                        SUM(duration_sec) / COUNT(*) AS average_duration
                        FROM (
                            SELECT channel_name, 
                            CASE
                                WHEN duration REGEXP '^PT[0-9]+H[0-9]+M[0-9]+S$' THEN 
                                TIME_TO_SEC(CONCAT(
                                SUBSTRING_INDEX(SUBSTRING_INDEX(duration, 'H', 1), 'T', -1), ':',
                            SUBSTRING_INDEX(SUBSTRING_INDEX(duration, 'M', 1), 'H', -1), ':',
                            SUBSTRING_INDEX(SUBSTRING_INDEX(duration, 'S', 1), 'M', -1)
                            ))
                                WHEN duration REGEXP '^PT[0-9]+M[0-9]+S$' THEN 
                                TIME_TO_SEC(CONCAT(
                                '0:', SUBSTRING_INDEX(SUBSTRING_INDEX(duration, 'M', 1), 'T', -1), ':',
                                SUBSTRING_INDEX(SUBSTRING_INDEX(duration, 'S', 1), 'M', -1)
                            ))
                                WHEN duration REGEXP '^PT[0-9]+S$' THEN 
                                TIME_TO_SEC(CONCAT('0:0:', SUBSTRING_INDEX(SUBSTRING_INDEX(duration, 'S', 1), 'T', -1)))
                                END AS duration_sec
                        FROM videos
                        ) AS subquery
                        GROUP BY channel_name""")
        df = pd.DataFrame(cursor.fetchall(),columns=cursor.column_names)                
        st.write(df)
        

elif questions == '10. Which videos have the highest number of comments, and what are their corresponding channel names?':
        cursor.execute("""SELECT Channel_Name,video_ID,Comment_Count
                            FROM videos
                            ORDER BY Comment_Count DESC
                            LIMIT 20""")
        df = pd.DataFrame(cursor.fetchall(),columns=cursor.column_names)
        st.write(df)
        fig = px.bar(df,
                     x=cursor.column_names[1],
                     y=cursor.column_names[2],
                     orientation='v',
                     color=cursor.column_names[0]
                    )
        st.plotly_chart(fig,use_container_width=True)        
       
