package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;

import android.content.ContentValues;
import android.net.Uri;
import android.os.AsyncTask;
import android.util.Log;

public class ServerTask extends AsyncTask<ServerSocket, String, Void>
{

	
	static final String URL = "content://edu.buffalo.cse.cse486586.simpledynamo.provider";  //Content URI providor
	static final Uri CONTENT_URI = Uri.parse(URL);
	Socket socket;
	SimpleDynamoProvider simpleDynamo=new SimpleDynamoProvider();
	String successor_portNumber="";
	String my_Port="";
	String FILENAME = "hello";
	int count=0;
	@Override
	protected Void doInBackground(ServerSocket... sockets)
	{
			Log.v("Server Task instance-->","welcome :)");
			while(true)
			{
					
					try
					{
							ServerSocket serverSocket = sockets[0];
				            socket=serverSocket.accept();  
				            BufferedReader br_Server=new BufferedReader(new InputStreamReader(socket.getInputStream()));
							String read=br_Server.readLine();
							successor_portNumber=SimpleDynamoProvider.finalS;
							my_Port=SimpleDynamoProvider.myPort;
							Log.v("Message Received to Check-->",read);
							if(read.contains("--"))
							{
								Log.v("Insert","Message Received to insert-->"+read);
								Log.v("Received by port-->",""+my_Port+""+read);
								ContentValues keyValueToInsert=new ContentValues();
								String splitMsgString[]=read.split("###");
								String keyValue[]=splitMsgString[1].split("--");
								//Log.v("Insert","Key to insert-->"+keyValue[0]);
								//Log.v("Insert"," Value to insert-->"+keyValue[1]);
								String key=keyValue[0];
								String value=keyValue[1];
								keyValueToInsert.put("value",value);
								keyValueToInsert.put("key",key);
								//Log.v("Insert","Insert in Server-->"+key);
								Uri uri_=simpleDynamo.insertKey(CONTENT_URI, keyValueToInsert);

							}
							else if(read.contains("!DEL#ALL#!"))
							{
								SimpleDynamoProvider.deleteAll();
							}
							else if(read.contains("!del!#"))
							{
								String getKeyToDelete[]=read.split("!del!#");
								SimpleDynamoProvider.deleteKey(getKeyToDelete[1]);
							}
							else if(read.contains("#IamBack#"))
							{
//								while((SimpleDynamoProvider.QueryDoFlag))
//								{
//									
//								}
								sendValuesSenderMissed(read);
								//Log.v("I m back","got msg");
							}
							else if(read.contains("!!"))
							{
								//Query All
								
									retrieveAllValues(read);
								//}
							}
							//To check if the Key is Present in Others
							else if(read.contains("ToGetVal###"))
							{
								Log.v("Query Check-->","Message Received to check-->"+read);
								StringBuffer valueToSend=new StringBuffer();
//								if(SimpleDynamoProvider.recoveryPhase)
//								{
								
									String queryValPort[]=read.split("ToGetVal###");
									String portToSend=queryValPort[0];
									String keyToLook=queryValPort[1];
									StringBuffer queryVal=new StringBuffer();
									/*while((SimpleDynamoProvider.QueryDoFlag))
									{

									}*/
									

									String value=simpleDynamo.queryDb(keyToLook);
										//Log.v("Query Check","Found at-->"+my_Port);
										//Log.v("Value Retrieved when not in recovery state-->",""+value);
										
										queryVal.append(value);
										queryVal.append("\n");
								  
										PrintWriter out=new PrintWriter(new OutputStreamWriter(socket.getOutputStream()));
									    out.print(queryVal.toString());
									    Log.v("Value sent back for","Key-->"+keyToLook+"Value-->"+queryVal);
									    out.flush();
									    out.close();
								

								
							}
				
							
							
					}
					catch(Exception e)
					{
						e.printStackTrace();
						
					}
			
			
		}
	}
	private void sendValuesSenderMissed(String read)
	{
		SimpleDynamoProvider smp=new SimpleDynamoProvider();
		StringBuffer valuesToReturn=new StringBuffer();
		try
		{
			if(read.contains("#MyPred"))
			{
				String predecessorPort[]=read.split("~~");
				
				//Log.v("at successor Port msg received-->",""+read);
				String predecessor[]=predecessorPort[1].split("#MyPred");
				String senderPort=predecessor[0];
				String predecessortoCheck=predecessor[1];
				//Log.v("Received Predecessors-->","Predecessor-->"+predecessortoCheck);
				//Log.v("Sender Port is-->","Sender Port is-->"+senderPort);
				
				valuesToReturn.append("WithPred");
				valuesToReturn.append("~~");
				valuesToReturn.append("#Sender#");
				valuesToReturn.append(senderPort);
				valuesToReturn.append("~~");
				valuesToReturn.append("#Prede#");
				valuesToReturn.append(predecessortoCheck);
				String valuesRetrieved=smp.SendingValuesToRecoverNode(valuesToReturn.toString());
				//Log.v("Values to Send to",""+senderPort+"is"+""+valuesRetrieved);
				StringBuffer returnToSender=new StringBuffer();
				PrintWriter writer = null;
				try
				{
					writer = new PrintWriter(new OutputStreamWriter(socket.getOutputStream()));
					//writer.print(valuesRetrieved+"\n");  
					 //write the message to output stream
					returnToSender.append(valuesRetrieved);
					returnToSender.append("\n");
					writer.print(returnToSender.toString());
					writer.flush();
					writer.close();
				} 
				catch (IOException e)
				{
					
					e.printStackTrace();
				}
			}
			else
			{
				String sendersPort[]=read.split("~~");
				String sender=sendersPort[1];
				//Log.v("Senders Port is-->","Snder is-->"+sender);
				//Log.v("Checking values at-->","predecessor");
				valuesToReturn.append(sender);
				String valuesRetrieved=smp.SendingValuesToRecoverNode(valuesToReturn.toString());
				StringBuffer returnToSender=new StringBuffer();
				PrintWriter writer = null;
				try
				{
					writer = new PrintWriter(new OutputStreamWriter(socket.getOutputStream()));
					
				} 
				catch (IOException e)
				{
					
					e.printStackTrace();
				}
				
				
				returnToSender.append(valuesRetrieved);
				returnToSender.append("\n");
				//Log.v("Values to Send to by predecessor","predec"+sender);
				writer.print(returnToSender.toString());  
				 //write the message to output stream
				writer.flush();
				writer.close();
			}
		
		}
		catch(Exception e)
		{
			Log.v("Exception thrown at Regex,","Check Properly");
			e.printStackTrace();
		}
		
		
		  
	}
	/*To Send And Retreieve All Values on * Query 
	 * */
	private void retrieveAllValues(String read) throws IOException
	{
		//Log.v("Query *","Message Received to check-->"+read);
	
		//String []portNo=read.split("!!");
		SimpleDynamoProvider smp=new SimpleDynamoProvider();
//		if(portNo[1].equalsIgnoreCase(successor_portNumber))
//		{
			
			//Log.v("Query *","Resend All values");
			String valuesRetrieved=smp.getValuesFromMyDb(read);    
			//Log.v("Query *","Value returning is-->"+valuesRetrieved);
			PrintWriter writer = null;
			try
			{
				writer = new PrintWriter(new OutputStreamWriter(socket.getOutputStream()));
			} 
			catch (IOException e)
			{
				
				e.printStackTrace();
			}
			StringBuffer valueReturned=new StringBuffer();
			valueReturned.append(valuesRetrieved);
			valueReturned.append("\n");
			//Log.v("Query *","Values Returning-->"+valueReturned.toString());
			writer.print(valueReturned.toString()+"\n");  
			 //write the message to output stream
			writer.flush();
			writer.close();
		
	}	

}
