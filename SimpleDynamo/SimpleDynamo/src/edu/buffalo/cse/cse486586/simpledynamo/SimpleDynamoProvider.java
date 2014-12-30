package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Vector;

import android.content.ContentProvider;
import android.content.ContentUris;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.SQLException;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider 
{

	//Vector<String> ringBased=new Vector<String>();
	HashMap<String,MyConnections> ringBased=new HashMap<String,MyConnections>();
	public static final String TABLE_NAME="ChatMessenger_table";
	static final String URL = "content://edu.buffalo.cse.cse486586.simpledynamo.provider";  //Content URI providor
	static final Uri CONTENT_URI = Uri.parse(URL);
	static int count=0;
	public static String mys;
	static final int DATABASE_VERSION = 1;
	static final String CREATE_TABLE =
			" CREATE TABLE " + TABLE_NAME +
			"( key TEXT, " +

			         	 	" value TEXT);";			//query to create the database table
	public static boolean QueryDoFlag=false;
	public static boolean recoveryPhase=false;
	public static SQLiteDatabase database;
	public int delCount=0;
	public static int no_of_avds=5;
	static final String DATABASE_NAME = "groupmessenger.db";			//name of the database table
	static final String DatabaseFullPath="/data/data/edu.buffalo.cse486586.simpledynamo/databases/groupmessenger.db";
	static final String DatabaseName="";
	static DatabaseHelper dbHelper;
	private static final String VALUE_FIELD = "value";
	private static final String KEY_FIELD ="key";
	static final int SERVER_PORT = 10000;
	static String myPort=null;	
	static Vector<MyConnections> allConnections=new Vector<MyConnections>();
	public static String AvdHashVal="";
	public static String mySuccesor="";
	public static String finalS="";
	public static int timeout=500;
	public static boolean insertFlag=true;
	
	/*
	 * Delete Query 
	 * */
	public int delete(Uri uri, String selection, String[] selectionArgs)
	{
		int rows=0;
		StringBuffer del=new StringBuffer();
		
		/*Send Message To All the AVDs to Delete resepctive Databse
		 * */
		if(selection.equalsIgnoreCase("*"))
		{
			StringBuffer getValues=new StringBuffer();
			getValues.append("!DEL#ALL#!");
			getValues.append("\n");
			for(int i=0;i<no_of_avds;i++)
			{
				
				MyConnections myc=allConnections.elementAt(i);

				String mygetPort=myc.getMyPort();
				Log.v("Delete","myGet port is-->"+mygetPort);

				Log.v("Delete","Sent port-->"+mygetPort+"to check");
				try
				{
					Socket skt = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(mygetPort));
					PrintWriter pw = new PrintWriter(new OutputStreamWriter(skt.getOutputStream()));
					pw.write(getValues.toString()+"\n");  
					pw.flush();
				}
				catch(Exception e)
				{
					Log.v("Delete","Socket exception at Delete");
				}
			}
			
			
		}
		/*To delete Self Database
		 * */
		else if(selection.equalsIgnoreCase("@"))
		{
			rows= database.delete(TABLE_NAME, null, null);
		}
		else
		{
			
			
				try
				{
					String hashKeyVal=genHash(selection);
					String myPortHashVal=assignPort(myPort);
					String mySuccessor1="";		//get My successor
					String mySuccessor2="";	//get my predecessor
					String highestHashVal=assignPort("11120");
					String lowestHashVal=assignPort("11124");
					Log.v("Inserting key-->" ,"" +selection);
					if((hashKeyVal.compareTo(highestHashVal))>=0 || ((hashKeyVal).compareTo(lowestHashVal)<=0))
					{
						StringBuffer msgToSend=new StringBuffer();
						msgToSend.append("!del!#");
						msgToSend.append(selection);
						msgToSend.append("\n");
						String success1="11112";
						String success2="11108";
						String leastPort="11124";
						try
						{
							Socket skt = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(leastPort));
							//skt.setSoTimeout(timeout);
							PrintWriter pw = new PrintWriter(new OutputStreamWriter(skt.getOutputStream()));
		
							pw.write(msgToSend.toString()); 
							pw.flush();
							Log.v("Delete"+ leastPort,""+msgToSend.toString());
							pw.close();
							skt.close();
							//pw.flush();
						}
						catch(Exception e)
						{
							Log.v("Delete Exception-->","Socket Exception at-->"+leastPort);
						}
						try
						{
							Socket skt_1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(success1));
							//skt_1.setSoTimeout(timeout);
							PrintWriter pw_1 = new PrintWriter(new OutputStreamWriter(skt_1.getOutputStream()));
			
			
							pw_1.write(msgToSend.toString()); 
							pw_1.flush();
							Log.v("Delete","2--Message sent to Delete at"+ success1+""+msgToSend.toString());
							pw_1.close();
							skt_1.close();
						}
						catch(Exception e)
						{
							Log.v("Delete Exception-->","Socket Exception at-->"+success1);
						}
						
						try
						{
							Socket skt_2 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(success2));
							//skt_2.setSoTimeout(timeout);
							PrintWriter pw_2 = new PrintWriter(new OutputStreamWriter(skt_2.getOutputStream()));
							pw_2.write(msgToSend.toString()); 
							pw_2.flush();
							pw_2.close();
							Log.v("Delete","3--Message sent to Delete at"+ success2+""+msgToSend.toString());
							skt_2.close();
						}
						catch(Exception e)
						{
							Log.v("Delete Exception-->","Socket Exception at-->"+success2);
							
						}
						
				
				}
				else
				{
					for(int i=0;i<5;i++)
					{

						MyConnections myCon=(MyConnections)allConnections.elementAt(i);
						String myPort1=myCon.getMyPort();
						
						String mySuccessor=myCon.getMySuccessor_1();
						if((hashKeyVal.compareTo(assignPort(myPort1))>0) && ((hashKeyVal).compareTo(assignPort(mySuccessor))<0))
						{
							callDeleteMethod(mySuccessor,selection);

						}
						

					}

				}
			
	//}
				}
				catch(Exception e)
				{
					e.printStackTrace();
				}
			
		}
		/*Return number of rows*/
		return rows;	
	}
	/*
	 * To Delete All Values
	 * */
	public static void deleteAll()
	{
		int rows= database.delete(TABLE_NAME, null, null);
		Log.v("Number of rows delete-->",""+rows);
	}
	
	/*To Delete Specific Key
	 * */
	public static void deleteKey(String key)
	{
		int rows= database.delete(TABLE_NAME, "key=?", new String[]{key});
		if(rows>0)
		{
			Log.v("Delete","Key Deleted at "+myPort+"is"+key);
		}
		
	}

	/*Method To determine which key to delete
	 * */
	private void callDeleteMethod(String mySuccessor, String selection) 
	{
		for(int j=0;j<5;j++)
		{

			MyConnections myCon=(MyConnections)allConnections.elementAt(j);
			String myPort2=myCon.getMyPort();
			if(myPort2.equalsIgnoreCase(mySuccessor))
			{
				String success1=myCon.getMySuccessor_1();
				String success2=myCon.getmySuccessor_2();
				StringBuffer msgToSend=new StringBuffer();
				msgToSend.append("!del!#");
				msgToSend.append(selection);
				msgToSend.append("\n");
				try
				{
					Socket skt = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(myPort2));
					//skt.setSoTimeout(timeout);
					PrintWriter pw = new PrintWriter(new OutputStreamWriter(skt.getOutputStream()));

					pw.write(msgToSend.toString()); 
					pw.flush();
					Log.v("Delete","4--Message sent to Delete at"+ myPort2+""+msgToSend.toString()+"--successor"+success1);
					pw.close();
					skt.close();
					//pw.flush();

					//Log.v("Insert","6 Starting to send--Message sent to Insert at"+ success1+""+msgToSend.toString());
					Socket skt_1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(success1));
					PrintWriter pw_1 = new PrintWriter(new OutputStreamWriter(skt_1.getOutputStream()));
					//skt_1.setSoTimeout(timeout);

					pw_1.write(msgToSend.toString()); 
					pw_1.flush();
					Log.v("Delete","5--Message sent to Delete at"+ success1+""+msgToSend.toString());
					pw_1.close();
					skt_1.close();                               

					Socket skt_2 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(success2));
					PrintWriter pw_2 = new PrintWriter(new OutputStreamWriter(skt_2.getOutputStream()));
					//skt_2.setSoTimeout(timeout);
					pw_2.write(msgToSend.toString()); 
					pw_2.flush();
					pw_2.close();
					Log.v("Delete","6--Message sent to Delete at"+ success2+""+selection);

					skt_2.close();
					count++;
					Log.v("Delete","Total values requested to Delete-->"+count);

				}
				catch(Exception e)
				{
					Log.v("Delete","Exception throw to send msg");
					e.printStackTrace();
				}
			}
		}

	}


	public String getType(Uri uri)
	{

		return null;
	}


	public Uri insert(Uri uri, ContentValues values) 
	{
		String values_Key=values.getAsString("key");
		String values_values=values.getAsString("value");
		//deleteExistingValues(values_Key);
		Log.v("Insert query shot-->","Key-->"+values_Key+"Values-->"+values_values);
		MyConnections myConnections= new MyConnections();
		try
		{
			String hashKeyVal=genHash(values_Key);
			String myPortHashVal=assignPort(myPort);
			String mySuccessor1="";		//get My successor
			String mySuccessor2="";	//get my predecessor
			String highestHashVal=assignPort("11120");
			String lowestHashVal=assignPort("11124");
			//Log.v("Inserting key-->" ,"" +values_Key);
			
			/*check to insert in self*/
			/*if((myPort.equalsIgnoreCase("11124")) || ((myPort).equalsIgnoreCase("11112"))||((myPort).equalsIgnoreCase("11108")))
			{
				if((hashKeyVal.compareTo(highestHashVal))>=0 || ((hashKeyVal).compareTo(lowestHashVal)<=0))
				{
					Cursor cursor=database.rawQuery("Select * from  ChatMessenger_table where key=?",new String[]{values_Key});
					cursor.moveToFirst();
					
					if(recoveryPhase)
					{
						while(SimpleDynamoProvider.QueryDoFlag)
						{
							Log.v("Retrieve Phase on-->",""+values_Key);
							
						}
					}
					
					if(cursor.getCount()>0)
					{
						Log.v("Values preexist sometime","Key is-->"+values_Key+"Value"+values_values);
						long rows=database.delete(TABLE_NAME,"key=?",new String[]{values_Key});
						
						
					}
					
					
					long row = database.insert(TABLE_NAME, "", values);	
				}
			}
			else
			{
				
					for(int j=0;j<5;j++)
					{

						MyConnections myCon=(MyConnections)allConnections.elementAt(j);
						String myPortk=myCon.getMyPort();
						String hashMyPort=assignPort(myPortk);
						if(myPort.equalsIgnoreCase(myPortk))
						{
							
							String pred=myCon.getMyPredeccessor();
							String myPredHash=assignPort(pred);
							{
								if((hashKeyVal.compareTo(myPredHash))>=0 || ((hashKeyVal).compareTo(hashMyPort)<=0))
								{
									Cursor cursor=database.rawQuery("Select * from  ChatMessenger_table where key=?",new String[]{values_Key});
									cursor.moveToFirst();
									
									if(recoveryPhase)
									{
										while(SimpleDynamoProvider.QueryDoFlag)
										{
											Log.v("Retrieve Phase on-->",""+values_Key);
											
										}
									}
									
									if(cursor.getCount()>0)
									{
										Log.v("Values preexist sometime","Key is-->"+values_Key+"Value"+values_values);
										long rows=database.delete(TABLE_NAME,"key=?",new String[]{values_Key});
										
										
									}
									
									
									long row = database.insert(TABLE_NAME, "", values);	
								}
								
							}
						}
					}
				//}
				
			}
			
			
			*/
			
			if((hashKeyVal.compareTo(highestHashVal))>=0 || ((hashKeyVal).compareTo(lowestHashVal)<=0))
			{
				StringBuffer msgToSend=new StringBuffer();
				msgToSend.append("###");
				msgToSend.append(values_Key);
				msgToSend.append("--");
				msgToSend.append(values_values);
				msgToSend.append("\n");
				String success1="11112";
				String success2="11108";
				String leastPort="11124";
				try
				{
					Socket skt = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(leastPort));
					//skt.setSoTimeout(timeout);
					PrintWriter pw = new PrintWriter(new OutputStreamWriter(skt.getOutputStream()));
	
					pw.write(msgToSend.toString()); 
					pw.flush();
					Log.v("Insert","1--Message sent to Insert at"+ leastPort+""+msgToSend.toString());
					pw.close();
					skt.close();
				//pw.flush();
				}
				catch(Exception e)
				{
					Log.v("Insert Exception-->","Socket Exception at-->"+leastPort);
				}
				try
				{
					Socket skt_1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(success1));
					//skt_1.setSoTimeout(timeout);
					PrintWriter pw_1 = new PrintWriter(new OutputStreamWriter(skt_1.getOutputStream()));
	
	
					pw_1.write(msgToSend.toString()); 
					pw_1.flush();
					Log.v("Insert","2--Message sent to Insert at"+ success1+""+msgToSend.toString());
					pw_1.close();
					skt_1.close();
				}
				catch(Exception e)
				{
					Log.v("Insert Exception-->","Socket Exception at-->"+success1);
				}
				
				try
				{
					Socket skt_2 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(success2));
					//skt_2.setSoTimeout(timeout);
					PrintWriter pw_2 = new PrintWriter(new OutputStreamWriter(skt_2.getOutputStream()));
					pw_2.write(msgToSend.toString()); 
					pw_2.flush();
					pw_2.close();
					Log.v("Insert","3--Message sent to Insert at"+ success2+""+msgToSend.toString());
					skt_2.close();
				}
				catch(Exception e)
				{
					Log.v("Insert Exception-->","Socket Exception at-->"+success2);
					
				}
				count++;
				Log.v("Insert","Total values sent-->"+count);

			}
			else
			{
				for(int i=0;i<5;i++)
				{

					MyConnections myCon=(MyConnections)allConnections.elementAt(i);
					String myPort=myCon.getMyPort();
					//if(!(myPort.equalsIgnoreCase("11120")))
					//{
					String mySuccessor=myCon.getMySuccessor_1();
					if((hashKeyVal.compareTo(assignPort(myPort))>0) && ((hashKeyVal).compareTo(assignPort(mySuccessor))<0))
					{
						callInsertMethod(mySuccessor,values_Key,values_values);

					}
					//}

				}

			}


		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		return null;
	}

	public void callInsertMethod(String successorPort,String key, String value)
	{
		for(int j=0;j<5;j++)
		{

			MyConnections myCon=(MyConnections)allConnections.elementAt(j);
			String myPort=myCon.getMyPort();
			if(myPort.equalsIgnoreCase(successorPort))
			{
				String success1=myCon.getMySuccessor_1();
				String success2=myCon.getmySuccessor_2();

				try
				{

					StringBuffer msgToSend=new StringBuffer();
					msgToSend.append("###");
					msgToSend.append(key);
					msgToSend.append("--");
					msgToSend.append(value);
					msgToSend.append("\n");
					
					try
					{
						Log.v("Asking port"+successorPort,"To Insert message-->"+msgToSend.toString());
						Socket skt = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(successorPort));
						//skt.setSoTimeout(timeout);
						PrintWriter pw = new PrintWriter(new OutputStreamWriter(skt.getOutputStream()));
	
						pw.write(msgToSend.toString()); 
						pw.flush();
						//Log.v("Insert else condition","4--Message sent to Insert at"+ successorPort+""+msgToSend.toString()+"--successor"+success1);
						pw.close();
						skt.close();
					}
					catch(Exception e)
					{
						Log.v("Insert Exception-->","Socket Exception at-->"+successorPort);
						e.printStackTrace();
					}
					//pw.flush();

					//Log.v("Insert","6 Starting to send--Message sent to Insert at"+ success1+""+msgToSend.toString());
					try
					{
						Log.v("Asking port"+success1,"To Insert message-->"+msgToSend.toString());
						Socket skt_1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(success1));
	
						PrintWriter pw_1 = new PrintWriter(new OutputStreamWriter(skt_1.getOutputStream()));
	
	
						pw_1.write(msgToSend.toString()); 
						pw_1.flush();
	
						pw_1.close();
						skt_1.close();
					}
					catch(Exception e)
					{
						Log.v("Insert Exception-->","Socket Exception at-->"+success1);
					}
					
					try
					{
						Socket skt_2 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(success2));
	
						PrintWriter pw_2 = new PrintWriter(new OutputStreamWriter(skt_2.getOutputStream()));
	
						pw_2.write(msgToSend.toString()); 
						pw_2.flush();
						pw_2.close();
						Log.v("Insert","6--Message sent to Insert at"+ success2+""+msgToSend.toString());
	
						skt_2.close();
					}
					catch(Exception e)
					{
						Log.v("Insert Exception-->","Socket Exception at-->"+success1);
					}
					count++;
					Log.v("Insert","Total values sent-->"+count);

				}
				catch(Exception e)
				{
					Log.v("Insert","Exception throw to send msg");
					e.printStackTrace();
				}
			}
		}
	}

	@Override
	/*Initializing various Parametrs on create
	 * */
	public boolean onCreate()
	{
		getMyPort();
		assignPortValues();
		Context context=getContext();	
		boolean dbExists=false;
		try
		{
			String dbFile=context.getDatabasePath(DATABASE_NAME).toString();
			Log.v("Database Path is-->",""+dbFile);
			dbExists=true;
		}
		catch(Exception e)
		{
			Log.v("could not open-->", "sorry");
			e.printStackTrace();
		}
		Log.v("Db Exists is-->",""+dbExists);
		try 
		{
			Log.v("inside create",Integer.toString(SERVER_PORT));
			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
		} 


		catch (IOException e)
		{

			Log.e("Error","Can't create a ServerSocket");
			e.printStackTrace();
			return false;
		}

		//Check if the database exists 


		boolean checkDbExists=checkIfDatabaseExists();
		Log.v("Databse Exists or Not-->",""+checkDbExists);
		//if exists,send message to everyone :) that I am back :)
		try
		{
			//initializing the context
			dbHelper = new DatabaseHelper(context);
			database = dbHelper.getWritableDatabase();

			if(database == null)
			{
				Log.v("Return value is-->","false");
				return false;
			}

		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		if(!(checkDbExists))
		{
			QueryDoFlag=false;
			recoveryPhase=false;
			Log.v("First Instance","created");
		}
		else
		{
			QueryDoFlag=true;
			recoveryPhase=true;
			Log.v("Already exists","no need to create");
			//long rows= database.delete(TABLE_NAME, null, null);
			createSocket();

			/*for(int i=0;i<no_of_avds;i++)
			{
				Log.v("My Connections size--->",""+allConnections.size());
				MyConnections myc=allConnections.elementAt(i);
				String imBack="Hi,Im Back";
				Log.v("Message to send-->",""+imBack);
				String mygetPort=myc.getMyPort();
				Log.v("My Port after recovery is-->",""+myPort);
				if(!(mygetPort.equalsIgnoreCase(myPort)))
				{
					Log.v("Sending Message","myGet port is-->"+mygetPort);

					try
					{

						Socket skt = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(mygetPort));

						PrintWriter pw = new PrintWriter(new OutputStreamWriter(skt.getOutputStream()));
						Log.v("Sending Message-->","msg sent-->"+imBack);
						pw.write(imBack+"\n");  
						pw.flush();


						try
						{
							pw.close(); 
						}
						catch(Exception e)
						{
							Log.v("Query *","Catch in printwriter");
						}

						try
						{
							skt.close(); 
						}
						catch(Exception e)
						{
							Log.v("Query *","Catch in socket close");
						}
					}
					catch(Exception e)
					{
						e.printStackTrace();
					}
				}
			}*/


		}


		return true;
	}

			//Check if the database exists 


		
//			Log.v("Databse Exists or Not-->",""+checkDbExists);
//			//if exists,send message to everyone :) that I am back :)
//			try
//			{
//				//initializing the context
//				dbHelper = new DatabaseHelper(context);
//				database = dbHelper.getWritableDatabase();
//
//				if(database == null)
//				{
//					Log.v("Return value is-->","false");
//					return false;
//				}
//
//			}
//			catch(Exception e)
//			{
//				e.printStackTrace();
//			}
//			
//		}
//		
//		try
//		{
//			String dbFile=context.getDatabasePath(DATABASE_NAME).toString();
//			Log.v("Database Path is-->",""+dbFile);
//			dbExists=true;
//		}
//		catch(Exception e)
//		{
//			Log.v("could not open-->", "sorry");
//			e.printStackTrace();
//		}
//		Log.v("Db Exists is-->",""+dbExists);
//
//		if(!(checkDbExists))
//		{
//			Log.v("First Instance","created");
//		}
//		else
//		{
//			Log.v("Already exists","no need to create");
//			//long rows= database.delete(TABLE_NAME, null, null);
//			createSocket();

			/*for(int i=0;i<no_of_avds;i++)
			{
				Log.v("My Connections size--->",""+allConnections.size());
				MyConnections myc=allConnections.elementAt(i);
				String imBack="Hi,Im Back";
				Log.v("Message to send-->",""+imBack);
				String mygetPort=myc.getMyPort();
				Log.v("My Port after recovery is-->",""+myPort);
				if(!(mygetPort.equalsIgnoreCase(myPort)))
				{
					Log.v("Sending Message","myGet port is-->"+mygetPort);

					try
					{

						Socket skt = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(mygetPort));

						PrintWriter pw = new PrintWriter(new OutputStreamWriter(skt.getOutputStream()));
						Log.v("Sending Message-->","msg sent-->"+imBack);
						pw.write(imBack+"\n");  
						pw.flush();


						try
						{
							pw.close(); 
						}
						catch(Exception e)
						{
							Log.v("Query *","Catch in printwriter");
						}

						try
						{
							skt.close(); 
						}
						catch(Exception e)
						{
							Log.v("Query *","Catch in socket close");
						}
					}
					catch(Exception e)
					{
						e.printStackTrace();
					}
				}
			}*/

	
	/*If value exists,Delete
	 * */

	public void deleteExistingValues(String key)
	{
		try
		{
			String var[]={key};
			Cursor cursor=database.rawQuery("Select * from  ChatMessenger_table where key=?",var);
			cursor.moveToFirst();
			
				if(cursor.getCount()>0)
				{
					Log.v("Key exists prior,deleting-->",""+key);
					long rows=database.delete(TABLE_NAME,"key=?",var);
					Log.v("Number of rows deleted-->",""+rows);
				}
		}
		catch(Exception e)
		{
			Log.v("Error deleting existing key","");
		}
		
	}
	
	private void createSocket()
	{
		Log.v("On recover Send Message to all","I am back");
		try
		{
			Log.v("Size of allConnections-->",""+allConnections.size());
			for(int j=0;j<no_of_avds;j++)
			{
				MyConnections mycon=allConnections.elementAt(j);
				String portVal=mycon.getMyPort();
				if(portVal.equalsIgnoreCase(myPort))
				{
					
				}
				else
				{
					new IamBack().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR);

					
				}
			}
			
			//Socket skt = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(mygetPort));

		}
		catch(Exception e)
		{
			Log.v("Socket creation","exception thrown");
			e.printStackTrace();
		}
		
	}


	private boolean checkIfDatabaseExists()
	{
		String DbPath="/data/data/edu.buffalo.cse.cse486586.simpledynamo/databases/groupmessenger.db";
		SQLiteDatabase sqlite;
		try
		{
			sqlite=SQLiteDatabase.openDatabase(DbPath, null, SQLiteDatabase.OPEN_READONLY);
			sqlite.close();
			return true;

		}
		catch(Exception e)
		{
			return false;
		}


	}


	@Override
	public Cursor query(Uri uri, String[] projection, String selection,String[] selectionArgs, String sortOrder) 
	{
		Log.v("Welcome to Key asked to Query-->","Key-->"+selection);

		database = dbHelper.getReadableDatabase();		//Retrieve the database to Query from SQLite Table
		Cursor cursor=null;
		
		Log.v("Welcome to Key asked to Query-->","Key-->"+selection);
		if(selection.equalsIgnoreCase("*"))
		{
//			while(SimpleDynamoProvider.QueryDoFlag)
//			{						
//			}
			StringBuffer getValues=new StringBuffer();
			Log.v("Query *","");
			
			
			getValues.append("!!");
			getValues.append(myPort);
			Log.v("Query *","started at-->"+myPort);
			StringBuffer gotVals=new StringBuffer();
			gotVals.append("!!");
			gotVals.append(myPort);
			//Cursor c=null;
			String columnNames[]={"key","value"};
			//			//Creating matrixCursor to store all objects
			MatrixCursor matrixCursor = new MatrixCursor(columnNames);
			try
			{

				for(int i=0;i<no_of_avds;i++)
				{

					MyConnections myc=allConnections.elementAt(i);

					String mygetPort=myc.getMyPort();
					Log.v("Query *","myGet port is-->"+mygetPort);

					Log.v("Query *","Sent port-->"+mygetPort+"to check");

					Socket skt = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(mygetPort));

					PrintWriter pw = new PrintWriter(new OutputStreamWriter(skt.getOutputStream()));
					pw.write(getValues.toString()+"\n");  
					pw.flush();

					
					BufferedReader buff= new BufferedReader(new InputStreamReader(skt.getInputStream()));
					try
					{
						String line = buff.readLine();
						if(line.length()>0)
						{
							gotVals.append(line.trim());
						}
						Log.v("Query *","Values Received-->"+line);
					}
					catch(Exception e)
					{
						Log.v("Failed to get Values-->","");
					}

					
					
					try
					{
						pw.close(); 
					}
					catch(Exception e)
					{
						Log.v("Query *","Catch in printwriter");
					}
					try
					{
						buff.close();
					}
					catch(Exception e)
					{
						Log.v("Query *","Catch in bufferwriter");

					}
					try
					{
						skt.close(); 
					}
					catch(Exception e)
					{
						Log.v("Query *","Catch in socket close");
					}

				}
				Log.v("Query *","Value received-->"+gotVals.toString());
				String[] a1=gotVals.toString().split("!!");
				String[] s3 =a1[1].split("##VS##");
				if(s3.length>1)
				{
					for(int j=1;j<s3.length;j++)
					{
						try
						{							
							String [] s4=s3[j].split("==");		
							if(s4[0].length()>0 && s4[1].length()>0)
							{
								String key=s4[0].trim();
								String value=s4[1].trim();
								matrixCursor.addRow(new String[]{key,value});
							}
	
						}
						catch(Exception e)
						{
							Log.v("Query *","Exception in for loop");
							e.printStackTrace();
						}
	
					}
				}
				

			}
			catch(Exception e)
			{
				Log.e("Query *", "Large Exception");
			}
			return matrixCursor;


		}
		else if(selection.equalsIgnoreCase("@"))
		{
			
				Log.v("Query @","@");
//				while(insertFlag)
//				{
//					
//				}
				//if(recoveryPhase)
				//{
//					while(SimpleDynamoProvider.QueryDoFlag)
//					{						
//					}
//				
					
					
					
					cursor=database.rawQuery("SELECT * FROM ChatMessenger_table", null);
					Log.v("Query","Number of Rows-->"+cursor.getCount());
					Log.v("Delete Counter is","delCount is-->"+delCount);
					for(int j=0;j<no_of_avds;j++)
					{
						MyConnections myCon=(MyConnections)allConnections.elementAt(j);
						String myPort2=myCon.getMyPort();
						if(myPort2.equalsIgnoreCase("11124") && (QueryDoFlag==true))
						{
							delCount++;
							
						}
						/*if(myPort2.equalsIgnoreCase("11124") && (delCount==2))
						{
							delete(CONTENT_URI, "*", null);
						}*/
					}
					return cursor;
					
				//}
			
		}
		else
		{
//			while(SimpleDynamoProvider.QueryDoFlag)
//			{						
//			}
			Log.v("Asking ports to check-->","Key-->"+selection);
//			cursor=database.rawQuery("Select * from  ChatMessenger_table where key=?",new String[]{selection});

	
//			while(SimpleDynamoProvider.QueryDoFlag)
//			{						
//			}
			
			//cursor=database.rawQuery("Select * from  ChatMessenger_table where key=?",var);
			//cursor.moveToFirst();
			
			/*if(recoveryPhase)
			{
				while(SimpleDynamoProvider.QueryDoFlag)
				{
					Log.v("Retrieve Phase on-->",""+values_Key);
					
				}
			}*/
			
				//Log.v("Values preexist sometime","Key is-->"+values_Key+"Value"+values_values);
				//Log.v("Exists in self-->","Key-->"+selection+"");
//				while(SimpleDynamoProvider.QueryDoFlag)
//				{
//					//Log.v("Retrieve Phase on-->",""+values_Key);
//					
//				}
				///cursor=database.query(SimpleDynamoProvider.TABLE_NAME, projection, "key=?", var, null, null, sortOrder);
				//Log.v("Cusor return-->","key-->")
//				cursor=database.rawQuery("Select * from  ChatMessenger_table where key=?",var);
//
//				
//				
//
//				Log.v("Retrieved in self and returning","Key-->"+selection+"Value-->"+cursor.getString(1));
//
//				if(cursor.getCount()>0)
//				{
//					Log.v("Retrieved in self and returning","Key-->"+selection+"Value-->"+cursor.getString(1));
//					return cursor;
//					
//				}
				
			//}
				//Log.v("Query Check","Successor static variable is-->"+mySuccesor);
				//String []var={selection};
			
//			if(cursor.getCount()>0)
//			{
//				int val=cursor.getCount();
//				Log.v("Value Returned-->",""+cursor);
//				Log.d("selection is:", selection);
//				cursor.moveToFirst();
//				String vals=cursor.getString(1);
//				Log.v("Value being returned by self is-->","key-->"+selection+"Value-->"+vals);
//				return cursor;
					//if(val>0)
					//{
//						while(insertFlag)
//						{
//							
//						}
//						if(recoveryPhase)
//						{
//							while(SimpleDynamoProvider.QueryDoFlag)
//							{
//								//cursor.moveToFirst();
//								
//							}
//							
//						}
						//String []var={selection};
						//cursor=database.query(SimpleDynamoProvider.TABLE_NAME, projection, "key=?", var, null, null, sortOrder);
						
//			}
//				
//			else 
//			{
				cursor=null;
				String hashKeyVal=null;
			//	Log.v("Query Check","checking in self and others");
				
				String [] var={selection};
				String myPortHashVal=assignPort(myPort);
				String mySuccessor1="";		//get My successor
				String mySuccessor2="";	//get my predecessor
				String highestHashVal=assignPort("11120");
				String lowestHashVal=assignPort("11124");
				
				try
				{
					hashKeyVal=genHash(selection);
				}
				catch(Exception e)
				{
					Log.v("Error in genHash","atQuery");
				}
				//Log.v("Query Check","sending message to successor");


				String hashKeyVal2="";
				try
				{
					hashKeyVal2=genHash(selection);
				}
				catch(Exception e)
				{
						Log.v("Exception in genhash-->",""+hashKeyVal2);
				}
				
				
				//Log.v("Query Check" ,"In others" +selection);
				String valueRetrieved="";
				StringBuffer msgToSend=new StringBuffer();
				String line=null;
				String line2=null;
				String line2k=null;
				Socket skt=null;
				BufferedReader buff=null;
				PrintWriter pw =null;
				if((hashKeyVal2.compareTo(highestHashVal))>=0 || ((hashKeyVal2).compareTo(lowestHashVal)<=0))
				{
					try
					{
						//String successor_port_1=getSuccessor();
						
						//Appending Values before Forwarding to successor
							msgToSend.append(myPort);
							msgToSend.append("ToGetVal###");
							msgToSend.append(selection+"\n");
							String Message=msgToSend.toString();
							Log.v("Query Check","Asking Port 11124 to check for-->"+selection);
							skt = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt("11124"));
							//skt.setSoTimeout(timeout);
							pw = new PrintWriter(new OutputStreamWriter(skt.getOutputStream()));
							pw.write(msgToSend.toString());  
							pw.flush();
							buff= new BufferedReader(new InputStreamReader(skt.getInputStream()));
							line = buff.readLine();
							//If 11124 failed to retrieve ask 11112
							
							if(line.equalsIgnoreCase("")||(line==null)|| line.length()==0)
							{
								Log.v("Value Failed to retrieve by 11124 is-->,Ask 11112",""+selection);
								try
								{
									Log.v("asking port 11112 to Query","As 11124 failed");
									Socket skt_2s = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt("11112"));
									//skt.setSoTimeout(timeout);
									PrintWriter pw_2s = new PrintWriter(new OutputStreamWriter(skt_2s.getOutputStream()));
									pw_2s.write(msgToSend.toString());  
									pw_2s.flush();
									BufferedReader buff2s=null;
									
									try
									{
										buff2s= new BufferedReader(new InputStreamReader(skt_2s.getInputStream()));
										String line2s = buff2s.readLine();
										buff2s.close();
										pw_2s.close();
										skt_2s.close();
										Log.v("11112 retreived value for-->","key"+selection+"--Value-->"+line);
										if(line2s.length()>2)
										{
											Log.v("Value retrieved by 11112 as -->","key-->"+selection+"value-->"+line2s);
											valueRetrieved=line2s.trim();
										}
										else
										{
											try
											{
												Log.v("Failed to retrieve in 11112","Asking 11108 is asked-->"+selection);
												Socket skt_2 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt("11108"));
												//skt.setSoTimeout(timeout);
												PrintWriter pw_2 = new PrintWriter(new OutputStreamWriter(skt_2.getOutputStream()));
												pw_2.write(msgToSend.toString());  
												pw_2.flush();
												
												BufferedReader buff2=null;
											
												try
												{
													buff2= new BufferedReader(new InputStreamReader(skt_2.getInputStream()));
													line2k = buff2.readLine();
													buff2.close();
													pw_2.close();
													skt_2.close();
													Log.v("11108 retreived value for when previous both returned nothing-->",""+selection+"as-->"+line2);
													if(line2k.length()>1)
													{
														Log.v("Value Retrieved by 11108-->","Key->"+selection+"Value-->"+line2k);
														
														valueRetrieved=line2k.trim();
														
													}
													
												}
												catch(Exception ex)
												{
													Log.v("Exception in successor while retrieveing","key-->"+selection);
													ex.printStackTrace();
												}
											}
											catch(Exception el)
											{
												el.printStackTrace();
											}
											
										}
									}
									catch(Exception ex)
									{
										Log.v("Exception in 11112 while retrieveing","key-->"+selection);
										try
										{
											Log.v("Asking 11108 in catch block to get the values-->",""+selection);
											Socket skt_2 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt("11108"));
											//skt.setSoTimeout(timeout);
											PrintWriter pw_2 = new PrintWriter(new OutputStreamWriter(skt_2.getOutputStream()));
											pw_2.write(msgToSend.toString());  
											pw_2.flush();
											
											BufferedReader buff2=null;
											buff2= new BufferedReader(new InputStreamReader(skt_2.getInputStream()));
											line2k = buff2.readLine();
											if(line2k.length()>2)
											{
												Log.v("11108 retreived value for when 11112 failed- ->",""+selection+"as-->"+line2);
 
												valueRetrieved=line2k.trim();
												
											}
											
											buff2.close();
											pw_2.close();
											skt_2.close();
											
											
										}
										catch(Exception es)
										{
											Log.v("Error Retreieving from 11108 for ",""+selection);
											es.printStackTrace();
											
										}
									}
									buff2s.close();
									pw_2s.close();
									skt_2s.close();
									
							}
							catch(Exception e)
							{
								Log.v("Exception caught in 11124,","For key-->"+selection);
								e.printStackTrace();
							}
								pw.close(); 
								buff.close();
								skt.close();
							}
							else
							{
								valueRetrieved=line.trim();
							}
								
						}
						catch(Exception e)
						{
							Log.v("Failed to retrieve in 11124,going to 11112",""+selection);
							try
							{
								Socket skt_2 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt("11112"));
								//skt.setSoTimeout(timeout);
								PrintWriter pw_2 = new PrintWriter(new OutputStreamWriter(skt_2.getOutputStream()));
								pw_2.write(msgToSend.toString());  
								pw_2.flush();
								
								BufferedReader buff2=null;
							
								try
								{
									buff2= new BufferedReader(new InputStreamReader(skt_2.getInputStream()));
									line2 = buff2.readLine();
									buff2.close();
									pw_2.close();
									skt_2.close();
									Log.v("11112 retreived value for-->",""+selection+"as-->"+line2);
									if(line2.length()>2 || line2==null)
									{
										valueRetrieved=line2.trim();
										
									}
									else
									{
										Log.v("Exception in 11112 while retrieveing","key-->"+selection);
										try
										{
											Log.v("Asking 11108 in catch block to get the values-->",""+selection);
											Socket skt_3 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt("11108"));
											//skt.setSoTimeout(timeout);
											PrintWriter pw_3 = new PrintWriter(new OutputStreamWriter(skt_3.getOutputStream()));
											pw_3.write(msgToSend.toString());  
											pw_3.flush();
											
											BufferedReader buff3=null;
										
											try
											{
												buff3= new BufferedReader(new InputStreamReader(skt_3.getInputStream()));
												line2k = buff2.readLine();
												
												buff3.close();
												pw_3.close();
												skt_3.close();
												if(line2k.length()>2)
												{
													valueRetrieved=line2k.trim();
													
												}
												Log.v("11108 retreived value for-->",""+selection+"as-->"+line2k);
											}
											catch(Exception ext)
											{
												Log.v("Exception in retrieving from 11108 while retrieveing","key-->"+selection);
												ext.printStackTrace();
											}
											
											
											
										}
										catch(Exception es)
										{
											Log.v("Error Retreieving from 11112 for ",""+selection);
											es.printStackTrace();
										}
										
									}
									
								}
								catch(Exception ex)
								{
									Log.v("Exception in successor while retrieveing","key-->"+selection);
									ex.printStackTrace();
								}
								
								
								
							}
							catch(Exception el)
							{
								Log.v("Exception thrown-->","when 11112 failed as a whole");
								el.printStackTrace();
							}
							try
							{
								pw.close(); 
								buff.close();
								skt.close();
							}
							catch(Exception ex)
							{
								Log.v("Exception while closing-->",""+selection);
							}
						}
						 
						/*if(line.length()>0)
						{
							Log.v("Found value in 11112",""+selection);
							Log.v("Key value pair from 11112 is-->",""+selection+"--"+line);
							valueRetrieved=line.trim();
							
						}
						else if(line2.length()>0)
						{

							Log.v("Found value in 11124",""+selection);
							Log.v("Key value pair from 11124 is-->",""+selection+"--"+line2);
							if(line2.length()>0)
							{
								valueRetrieved=line2.trim();
							}
						}
						else if(line2k.length()>1)
						{
							Log.v("Found value in 11108",""+selection);
							Log.v("Key value pair from 11108 is-->",""+selection+"--"+line2);
							if(line2k.length()>0)
							{
								valueRetrieved=line2k.trim();
							}
						}*/
					}
					

				
				else
				{
					for(int i=0;i<5;i++)
					{

						MyConnections myCon=(MyConnections)allConnections.elementAt(i);
						String myPort=myCon.getMyPort();
						String mySuccessor=myCon.getMySuccessor_1();
						if((hashKeyVal.compareTo(assignPort(myPort))>0) && ((hashKeyVal).compareTo(assignPort(mySuccessor))<0))
						{
							Log.v("Checking for value in others except 3 spl ones-->",""+selection);
							valueRetrieved=callInsertMethod(mySuccessor,selection);
							if(valueRetrieved.equalsIgnoreCase("")|| valueRetrieved.length()==0 || valueRetrieved==null)
							{
								valueRetrieved=askSuccessor(selection,mySuccessor);
								Log.v("Query","Value Retrieved for key"+selection+"Value"+valueRetrieved);
							}

						}


					}

				}


				if(valueRetrieved.equalsIgnoreCase(""))
				{
					//Log.v("Failed to retrieve value from any-->",""+selection);
					//Log.v("Query Check",valueRetrieved);
					String columnNames[]={"key","value"};
					Log.v("Returning values as-->","Key-->"+selection+"Value-->"+valueRetrieved);
					MatrixCursor matrixCursor = new MatrixCursor(columnNames);
					if(selection.length()>0 && valueRetrieved.length()>0)
					{
						matrixCursor.addRow(new String[]{selection.trim(),valueRetrieved.trim()});
					}
					cursor=matrixCursor;
					return cursor;

				}
				else
				{
					//Log.v("Query Check","Value found");
					String columnNames[]={"key","value"};
					Log.v("Value sending back from others is key as","key-->"+selection+"--"+"value-->"+valueRetrieved);
					MatrixCursor matrixCursor = new MatrixCursor(columnNames);
					if(selection.length()>0 && valueRetrieved.length()>0)
					{
						matrixCursor.addRow(new String[]{selection.trim(),valueRetrieved.trim()});
					}
					cursor=matrixCursor;
					return cursor;
				}

			//}
				
		//}
		//;
			
		}
			
	}


	private String askSuccessor(String selection,String myPort) 
	{
		Log.v("Failed to retrieve where it belongs-->",""+selection);
		StringBuffer msg=new StringBuffer();
		//Appending Values before Forwarding to successor
		msg.append(myPort);
		msg.append("ToGetVal###");
		msg.append(selection+"\n");
		String Message=msg.toString();
		StringBuffer valReturn=new StringBuffer();
		for(int i=0;i<no_of_avds;i++)
		{
			MyConnections myCon=(MyConnections)allConnections.elementAt(i);
			String myPort2=myCon.getMyPort();
			if(myPort.equalsIgnoreCase(myPort2))
				
			{
				String successor1=myCon.getMySuccessor_1();
				String successor2=myCon.getMyPredeccessor_2();
				try
				{
					Log.v("Query check-->","Asking port to check,as actual Coordinator failed"+successor1+"for key-->"+selection);
					Socket skt = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(successor1));
					//skt.setSoTimeout(timeout);
					PrintWriter pw = new PrintWriter(new OutputStreamWriter(skt.getOutputStream()));
					pw.write(msg.toString());  
					pw.flush();
					BufferedReader buff= new BufferedReader(new InputStreamReader(skt.getInputStream()));
					String line = buff.readLine();
					
					
					buff.close();
					pw.close();
					skt.close();
					Log.v("Value received from actual owner-->","key"+selection+"Value"+line);
				if(line.equalsIgnoreCase("")|| line==null)
				{
					Log.v("Port-->"+successor1+"","Failed to retrieve for-->"+selection);
					Socket skt2 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(successor2));
					//skt.setSoTimeout(timeout);
					PrintWriter pw2 = new PrintWriter(new OutputStreamWriter(skt2.getOutputStream()));
					pw2.write(msg.toString());  
					pw2.flush();
					BufferedReader buff2= new BufferedReader(new InputStreamReader(skt2.getInputStream()));
					String line2 = buff2.readLine();
					
					valReturn.append(line2);
					
					Log.v("Query","Key-Value Pair from Co-ord Succesor's successor-->"+selection+"Value"+line);
					buff2.close();
					pw2.close();
					skt2.close();
				}
				else
				{
					Log.v("Query","Key-Value Pair from Co-ord Succesor-->"+selection+"Value"+line);
					
					valReturn.append(line);
					return valReturn.toString();
				}
						
					
				}
				catch(Exception e)
				{
					Log.v("Exception Caught while Asking Successors to check",""+selection);
					try
					{
						Socket skt2 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(successor2));
						//skt.setSoTimeout(timeout);
						PrintWriter pw2 = new PrintWriter(new OutputStreamWriter(skt2.getOutputStream()));
						pw2.write(msg.toString());  
						pw2.flush();
						BufferedReader buff2= new BufferedReader(new InputStreamReader(skt2.getInputStream()));
						String line2 = buff2.readLine();
						buff2.close();
						pw2.close();
						skt2.close();
						if(line2.length()>1)
						{
							valReturn.append(line2.trim());
						}
						
						Log.v("Query","Key-Value Pair from Co-ord Succesor's successor-->"+selection+"Value"+line2);
						return valReturn.toString();
					}
					catch(Exception s)
					{
						Log.v("Next sucessor failed to retrieve as well-->","key"+selection);
						s.printStackTrace();
					}
				}
			}
		}
		return valReturn.toString();
	}
	@SuppressWarnings("finally")
	private String callInsertMethod(String mySuccessor, String selection) 
	{
		StringBuffer stringToReturn=new StringBuffer();
		PrintWriter pw=null;
		Socket skt=null;
		BufferedReader buff=null;
		for(int j=0;j<5;j++)
		{

			MyConnections myCon=(MyConnections)allConnections.elementAt(j);
			String myPort=myCon.getMyPort();
			if(myPort.equalsIgnoreCase(mySuccessor))
			{
				try
				{
					String successor_port_1=myCon.getMySuccessor_1();
					
					StringBuffer msgToSend=new StringBuffer();
					//Appending Values before Forwarding to successor
					msgToSend.append(myPort);
					msgToSend.append("ToGetVal###");
					msgToSend.append(selection+"\n");
					String Message=msgToSend.toString();
					Log.v("Query check-->","Asking port"+mySuccessor+"for key-->"+selection);
					skt = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(mySuccessor));
					//skt.setSoTimeout(timeout);
					pw = new PrintWriter(new OutputStreamWriter(skt.getOutputStream()));
					pw.write(msgToSend.toString());  
					pw.flush();

					//Creating BufferedWriter to read incoming Values
					String line="";
					String line2="";
					
					try
					{
						buff= new BufferedReader(new InputStreamReader(skt.getInputStream()));
						line = buff.readLine();
						buff.close();
						pw.close();
						skt.close();
						if(line.length()>0)
						{
							Log.v("Value received from actual owner-->","key"+selection+"Value"+line);
							stringToReturn.append(line.trim());
							return stringToReturn.toString();
							
						}
					}
					catch(Exception e)
					{
						Log.v("Retrieval failed from owner,ask successor-->",""+selection);
						Socket skt_2 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(successor_port_1));
						//skt.setSoTimeout(timeout);
						PrintWriter pw_2 = new PrintWriter(new OutputStreamWriter(skt_2.getOutputStream()));
						Log.v("Message sending to successorPort-->",""+successor_port_1+"key-->"+selection);
						
						pw_2.write(msgToSend.toString());  
						pw_2.flush();
						
						BufferedReader buff2=null;
						
						try
						{
							buff2= new BufferedReader(new InputStreamReader(skt_2.getInputStream()));
							line2 = buff2.readLine();
							Log.v("Value Received from successor as","key"+selection+"Value-->"+line2);
							if(line2.length()>0)
							{
								stringToReturn.append(line2.trim());
							}
						}
						catch(Exception ex)
						{
							Log.v("Exception in successor while retrieveing from successor","key-->"+selection);
							ex.printStackTrace();
						}
						buff2.close();
						pw_2.close();
						skt_2.close();
						
					}
					//Log.v("Query Check",""+line);
				

				}
				catch(Exception e)
				{
					Log.v("Exception thrown to lookup Query-->",""+selection);
					e.printStackTrace();
					return "";
				}
				finally
				{
					try
					{
						pw.close(); 
						buff.close();
						skt.close(); 
					}
					catch(Exception e)
					{
						e.printStackTrace();
					}
					
				
					
					
					return stringToReturn.toString();
				}
			}
		}
		return "";
	}


	


	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) 
	{

		return 0;
	}

	private String genHash(String input) throws NoSuchAlgorithmException 
	{
		MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
		byte[] sha1Hash = sha1.digest(input.getBytes());
		Formatter formatter = new Formatter();
		for (byte b : sha1Hash) 
		{
			formatter.format("%02x", b);
		}
		return formatter.toString();
	}

	/*Assigning Port Values of respective Port to their successors
	 * */
	public void assignPortValues()
	{
		String portVal[]= {"5554","5556","5558","5560","5562"};
		try
		{

			Log.v("SimpleDHT","My port val is--"+myPort);
			MyConnections n1=new MyConnections();
			n1.setMyPort("11108");
			n1.setMySuccessor_1("11116");
			n1.setmySuccessor_2("11120");
			n1.setMyPredeccessor("11112");
			n1.setMyPredeccessor_2("11124");

			allConnections.add(n1);

			MyConnections n2=new MyConnections();
			n2.setMyPort("11112");
			n2.setMySuccessor_1("11108");
			n2.setmySuccessor_2("11116");
			n2.setMyPredeccessor("11124");
			n2.setMyPredeccessor_2("11120");
			allConnections.add(n2);

			MyConnections n3=new MyConnections();
			n3.setMyPort("11116");
			n3.setMySuccessor_1("11120");
			n3.setmySuccessor_2("11124");
			n3.setMyPredeccessor("11108");
			n3.setMyPredeccessor_2("11112");

			allConnections.add(n3);

			MyConnections n4=new MyConnections();
			n4.setMyPort("11120");
			n4.setMySuccessor_1("11124");
			n4.setmySuccessor_2("11112");
			n4.setMyPredeccessor("11116");
			n4.setMyPredeccessor_2("11108");
			allConnections.add(n4);

			MyConnections n5=new MyConnections();
			n5.setMyPort("11124");
			n5.setMySuccessor_1("11112");
			n5.setmySuccessor_2("11108");
			n5.setMyPredeccessor("11120");
			n5.setMyPredeccessor_2("11116");
			allConnections.add(n5);

			for(int i=0;i<5;i++)
			{
				MyConnections n=allConnections.elementAt(i);
				String myp=n.getMyPort();
				mys=n.getMySuccessor_1();
				String mypr=n.getmySuccessor_2();

				if(myp.equalsIgnoreCase(myPort))
				{
					Log.v("successor is->",mys);
					Log.v("My Predecessor is-->",n.getMyPredeccessor());
					Log.v("My previous predeccosr is-->",n.getMyPredeccessor_2());
					finalS=mys;
					Log.v("My predeccor is-->",mypr);
				}
			}


		}
		catch(Exception e)
		{
			e.printStackTrace();
		}



	}

	//Assign My Port Value to My Port
	public void getMyPort()
	{
		TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		myPort = String.valueOf((Integer.parseInt(portStr) * 2));
		Log.v("My Port is-->",""+myPort);
	}

	public String assignPort(String myPort)
	{

		try
		{

			if(myPort.equalsIgnoreCase("11108"))
			{
				AvdHashVal=genHash("5554");

			}
			else if(myPort.equalsIgnoreCase("11112"))
			{
				AvdHashVal=genHash("5556");
			}
			else if(myPort.equalsIgnoreCase("11116"))
			{
				AvdHashVal=genHash("5558");
			}
			else if(myPort.equalsIgnoreCase("11120"))
			{
				AvdHashVal=genHash("5560");
			}
			else if(myPort.equalsIgnoreCase("11124"))
			{
				AvdHashVal=genHash("5562");
			}
		}
		catch(Exception e)
		{
			Log.v("GenHashError",""+AvdHashVal);
			e.printStackTrace();
		}
		return AvdHashVal;
	}

	//To Insert Values in Databse after Determing It Belongs
	public  Uri insertKey(Uri uri, ContentValues values)
	{

		insertFlag=true;
		//Log.v("Insert","Insert method in Database");
		//Log.v("Insert","TableName-->"+TABLE_NAME);
		//Log.v("Insert",""+values.size());
		String values_Key=values.getAsString("key");
		String values_values=values.getAsString("value");
		String []var={values_Key};
		
		Cursor cursor=database.rawQuery("Select * from  ChatMessenger_table where key=?",var);
		//cursor.moveToFirst();
		
		/*if(recoveryPhase)
		{
			while(SimpleDynamoProvider.QueryDoFlag)
			{
				Log.v("Retrieve Phase on-->",""+values_Key);
				
			}
		}*/
		
		if(cursor.getCount()>0)
		{
			//Log.v("Values preexist sometime","Key is-->"+values_Key+"Value"+values_values);
			long rows=database.delete(TABLE_NAME,"key=?",var);
			
			
		}
		
		Log.v("Value getting inserted-->","Key-->"+values_Key+"Values-->"+values_values);
		//long row=database.rawQuery(sql, selectionArgs, cancellationSignal);
		long row = database.insert(TABLE_NAME, "", values);			
		//Log.v("Insert","Content URI is-->"+CONTENT_URI.toString());
		Uri newUri = ContentUris.withAppendedId(CONTENT_URI, row);

		getContext().getContentResolver().notifyChange(newUri, null);

		insertFlag=false;
		return newUri;



	}

	public String getSuccessor()
	{
		for(int k=0;k<5;k++)
		{
			MyConnections mycon=new MyConnections();
			Log.v("My Port is-->",""+myPort);
			if(myPort.equalsIgnoreCase(mycon.getMyPort()))
			{
				mySuccesor=mycon.getMySuccessor_1();
				Log.v("My Successor is-->",""+mySuccesor);
				return mySuccesor;
			}
		}
		return mySuccesor;
	}

	/*To retrieve values from My Database
	 * */
	public String getValuesFromMyDb(String readValues) 
	{

		Cursor cursor;
		StringBuffer getValues=new StringBuffer();
		//getValues.append(readValues);
		cursor=database.rawQuery("SELECT * FROM ChatMessenger_table", null);
		//Log.v("Values Getting inserted are-->","");
		if(cursor.getCount()>0)
		{
			if (cursor.moveToFirst()) 
			{
				do
				{
					//Extracting the Key and Value 
					int keyIndex = cursor.getColumnIndex(KEY_FIELD);
					int valueIndex = cursor.getColumnIndex(VALUE_FIELD);
					String returnKey = cursor.getString(keyIndex);
					String returnValue = cursor.getString(valueIndex);
					getValues.append("##VS##");
					getValues.append(returnKey);
					getValues.append("==");
					getValues.append(returnValue);


				} while (cursor.moveToNext());
			}

		}
		return getValues.toString();
	}

	

	/*To Query Database for a Key
	 * 
	 * */
	public String queryDb(String keyVal)
	{
		try
		{
			/*while(SimpleDynamoProvider.QueryDoFlag)
			{
				
			}*/
			//Log.d("searching in myPort",myPort);
			database=dbHelper.getReadableDatabase();	
			String query = "Select * FROM " + TABLE_NAME + " WHERE  key =? ";
			String[] ary = new String[] { keyVal };
			Cursor cursor=database.rawQuery(query, ary);
			//If value Value Exists in The Querying Android
			//Log.v("Key found count-->",""+cursor.getCount());
			if(cursor.getCount()>0)
			{
				cursor.moveToFirst();
				String value=cursor.getString(1);
				Log.v("Value of the key","Key-->"+keyVal+"--value-->"+value.trim());
				return value;

			}
			else
			{
				return "";
			}

		}
		catch(Exception e)
		{
			e.printStackTrace();
			return null;
		}



	}
	
	/*For * Query, Retreive all Values*/
	public Cursor getAllValues(String key)
	{
		StringBuffer gotVals=new StringBuffer();
		Cursor c=null;
		String columnNames[]={"key","value"};
		//		//Creating matrixCursor to store all objects
		MatrixCursor matrixCursor = new MatrixCursor(columnNames);
		try
		{

			gotVals.append(key);
			for(int i=0;i<no_of_avds;i++)
			{

				MyConnections myc=allConnections.elementAt(i);

				String mygetPort=myc.getMyPort();
				//Log.v("Query *","myGet port is-->"+mygetPort);
				
				//Log.v("Query *","Sent port-->"+mygetPort+"to check");
				//Log.v("Query *","Message Sending is-->"+gotVals.toString());
				Socket skt = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(mygetPort));
			
				PrintWriter pw = new PrintWriter(new OutputStreamWriter(skt.getOutputStream()));
				pw.write(gotVals.toString()+"\n");  
				pw.flush();

				BufferedReader buff= new BufferedReader(new InputStreamReader(skt.getInputStream()));
				String line = buff.readLine();
				//Log.v("Query *",""+line);
				//gotVals.append(line);
				//Log.v("Query *","Value received-->"+line);
				String[] a1=line.split("!!");
				String[] s3 =a1[1].split("##VS##");
				//splitting string to retrieve values
				for(int j=1;j<s3.length;i++)
				{
					try
					{

						String [] s4=s3[j].split("==");

						matrixCursor.addRow(new String[]{s4[0],s4[1]});
						//Log.v("Query *","Value sending-->"+s4[1]);
						//Log.v("Query *","Key Sending-->"+s4[0]);
					}
					catch(Exception e)
					{
						Log.v("Query *","Exception in for loop");
						e.printStackTrace();
					}

				}					
				pw.close(); 
				buff.close();
				skt.close(); 
				
			}
			try
			{
				c=matrixCursor;
				c.moveToFirst();
			}
			catch(Exception e)
			{
				Log.v("Query *","Error assigning values");
				e.printStackTrace();
			}
			return c;
		}
		catch(Exception e)
		{
			Log.v("Query *","Total Method Exception");
			e.printStackTrace();
			return c;
		}
		finally
		{
			//Log.v("Query *","Cursor closed");
			c.close();
		}
		//return  gotVals.toString();
	}
	
	/*To retrieve values from My Database
	 * */
	public String SendingValuesToRecoverNode(String readValues) 
	{
	
		Cursor cursor;
		StringBuffer getValues=new StringBuffer();
		//Log.v("The value to recover is-->","values are-->"+readValues);
		if(readValues.contains("WithPred"))
		{
			String getAllData[]=readValues.split("~~");
			String sendervals[]=getAllData[1].split("#Sender#");
			String predVals[]=getAllData[2].split("#Prede#");
			String senderPort=sendervals[1];
			String predecessorPort=predVals[1];
			//Log.v("Sender whose hash range to check-->",""+senderPort);
			//Log.v("Predecesor value is of sender @ sucessor-->",""+predecessorPort);
			
			cursor=database.rawQuery("SELECT * FROM ChatMessenger_table", null);
			String hashsender=assignPort(senderPort);
			String hashPredecessor=assignPort(predecessorPort);
			if(cursor.getCount()>0)
			{
				try
				{
					if (cursor.moveToFirst()) 
					{
						do
						{
							//Extracting the Key and Value 
							int keyIndex = cursor.getColumnIndex(KEY_FIELD);
							int valueIndex = cursor.getColumnIndex(VALUE_FIELD);
							String returnKey = cursor.getString(keyIndex);
							String returnValue = cursor.getString(valueIndex);
							String keyHashVal=genHash(returnKey);
							if(senderPort.equalsIgnoreCase("11124"))
							{
								//Log.v("11124 sender Port-->","Condition to handle for 11124");
								String hashSuccess=assignPort("11120");
								String hashLowest=assignPort("11124");
								if((keyHashVal).compareTo(hashSuccess)>=0 || ((keyHashVal).compareTo(hashLowest)<=0))
								{
									getValues.append("##VS##");
									getValues.append(returnKey);
									getValues.append("==");
									getValues.append(returnValue);
									Log.v("Sending Values to 11124-->","Key-->"+returnKey+"Value-->"+returnValue);
								}
								
								//Log.v("Retreievig Values for 11124",""+getValues.toString());
							}
							if((keyHashVal.compareTo(hashsender)<=0) && (keyHashVal.compareTo(hashPredecessor)>=0))
							{
								getValues.append("##VS##");
								getValues.append(returnKey.trim());
								getValues.append("==");
								getValues.append(returnValue.trim());
								Log.v("Sending Values to "+senderPort+"-->","Key-->"+returnKey+"Value-->"+returnValue);

								
							}
							
	
	
						}while (cursor.moveToNext());
					}
	
			}
			catch(Exception e)
			{
				Log.v("Exception at successor Retreival","While retreieving keys to send");
				e.printStackTrace();
			}
			
			}return getValues.toString();
		}
		else
		{
			//Retrieve Values from Predecessor
			//Log.v("Sender Port is at predecessor-->",""+readValues);
			cursor=database.rawQuery("SELECT * FROM ChatMessenger_table", null);
			if(cursor.getCount()>0)
			{
				String myPortHash=assignPort(myPort);
				//Log.v("I have to check-->","My Port val is-->"+myPort);
				String senderPort=readValues;
				//Log.v("Sender Port val is-->","My Port val is-->"+senderPort);
				String hashsender=assignPort(senderPort);
				String pred1=null;
				String pred2=null;
				for(int k=0;k<no_of_avds;k++)
				{
					MyConnections mycon=SimpleDynamoProvider.allConnections.elementAt(k);
					if(senderPort.equals(mycon.getMyPort()))
					{
						pred1=mycon.getMyPredeccessor();
						pred2=mycon.getMyPredeccessor_2();
					}
				}
				try
				{
					for(int j=0;j<no_of_avds;j++)
					{
						
						MyConnections mycon=SimpleDynamoProvider.allConnections.elementAt(j);
						String portVal=mycon.getMyPort();
						if(portVal.equalsIgnoreCase(SimpleDynamoProvider.myPort))
						{
							//Log.v("Inside checking values-->",""+portVal);
							String myPred=mycon.getMyPredeccessor();
							String hashPred=assignPort(myPred);
							if(cursor.moveToNext())
							{
								do
								{
									//Extracting the Key and Value 
									int keyIndex = cursor.getColumnIndex(KEY_FIELD);
									int valueIndex = cursor.getColumnIndex(VALUE_FIELD);
									String returnKey = cursor.getString(keyIndex);
									//Log.v("Key value Retrieved-->",""+returnKey);
									String returnValue = cursor.getString(valueIndex);
									//Log.v("Value retrieved-->",""+returnValue);
									String keyHashVal=genHash(returnKey);
									if(senderPort.equalsIgnoreCase("11124") || pred1.equalsIgnoreCase("11124") || pred2.equalsIgnoreCase("11124"))
									{
										String hashSuccess=assignPort("11120");
						//				Log.v("Recovery","Port is 11124");
										if((keyHashVal).compareTo(hashSuccess)>=0 || ((keyHashVal).compareTo(hashsender)<=0))
										{
											getValues.append("##VS##");
											getValues.append(returnKey);
											getValues.append("==");
											getValues.append(returnValue);
											Log.v("Sending Values to 11124-->","Key-->"+returnKey+"Value-->"+returnValue);

											
										}
									}
									if((keyHashVal.compareTo(myPortHash)<=0) && (keyHashVal.compareTo(hashPred)>=0))
									{
										
										//Log.v("Keys sending to sender-->",""+returnKey);
										//Log.v("Values sending to sender-->",""+returnKey);
										getValues.append("##VS##");
										getValues.append(returnKey.trim());
										getValues.append("==");
										getValues.append(returnValue.trim());
										Log.v("Sending Values to"+senderPort+"-->","Key-->"+returnKey+"Value-->"+returnValue);

										
									}
									
								} while (cursor.moveToNext());
							}
							
							
						}
					}
			}
			catch(Exception e)
			{
				Log.v("Exception in retreieving values to send back-->","");
				e.printStackTrace();
			}
			return getValues.toString();
			}
		}
		return "";
	
	
		
		
	}


	/*Inserting values in Database after recovery*/
	public String insertValuesRetrieved(String line)
	{
		
		//Log.v("In insert DB method","");
		int count=0;
		if(line.length()>0)
		{
			
			//Log.v("Line length >0 for key-->","");
			try
			{
			String[] s3 =line.trim().split("##VS##");
			//splitting string to retrieve values
			//Log.v("Retrieved Values are-->",""+line);
			for(int j=1;j<s3.length;j++)
			{
				try
				{
					count++;
					String [] s4=s3[j].split("==");
					String key=s4[0];
					String value=s4[1];
					//Log.v("Key to after receieving--->",""+key);
					//Log.v("Key to insert after recovery-->",""+key);
					//Log.v("Value to insert after recovery-->",""+value);
					//deleteExistingValues(key);
					
					ContentValues keyValueToInsert=new ContentValues();
					keyValueToInsert.put("value",value.trim());
					keyValueToInsert.put("key",key.trim());
				//	Log.v("Key inserting after recovery-->",""+key+"Value-->"+value.trim());

					insertKey(CONTENT_URI, keyValueToInsert);
					
				}
				catch(Exception e)
				{
					Log.v("Recovery","Exception to insert");
					e.printStackTrace();
				}
	
			}	
			//Log.v("Number of key-value pair inserted-->",""+count);
			return "success";
			}
			catch(Exception e)
			{
				Log.v("Exception while split-->","Split probably");
			}
		}
		return null;
		//return newUri;
		
	/*	for(int j=1;j<s3.length;i++)
		{
			try
			{

				String [] s4=s3[j].split("==");

				matrixCursor.addRow(new String[]{s4[0],s4[1]});
				Log.v("Query *","Value sending-->"+s4[1]);
				Log.v("Query *","Key Sending-->"+s4[0]);
			}
			catch(Exception e)
			{
				Log.v("Query *","Exception in for loop");
				e.printStackTrace();
			}

		}	*/	
	}
	
}
