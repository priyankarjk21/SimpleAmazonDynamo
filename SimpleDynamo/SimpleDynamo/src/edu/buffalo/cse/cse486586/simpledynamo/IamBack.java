package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;

import android.net.Uri;
import android.os.AsyncTask;
import android.util.Log;

public class IamBack extends AsyncTask<ServerSocket, String, Void>{

	public static int flag=0;
	protected Void doInBackground(ServerSocket... params)
	{
		try
		{
			Log.v("IamBack","Enter I am back Task");
			StringBuffer requestFailedVals=new StringBuffer();
			requestFailedVals.append("#IamBack#");
			requestFailedVals.append("~~");
			
			StringBuffer requestFailedToPredecessor=new StringBuffer();
			requestFailedToPredecessor.append("#IamBack#");
			requestFailedToPredecessor.append("~~");
			for(int j=0;j<5;j++)
			{
				MyConnections mycon=SimpleDynamoProvider.allConnections.elementAt(j);
				String portVal=mycon.getMyPort();
				if(portVal.equalsIgnoreCase(SimpleDynamoProvider.myPort))
				{
					requestFailedVals.append(portVal);
					requestFailedToPredecessor.append(portVal);
					String successor_1=mycon.getMySuccessor_1();
					String successor_2=mycon.getMyPredeccessor_2();
					String myPredeccesor=mycon.getMyPredeccessor();
					String myPredecessor2=mycon.getMyPredeccessor_2();
					Log.v("Recovery","My successor is-->"+successor_1);
					Log.v("Recovery","My Predecessor is-->"+myPredeccesor);
					Log.v("Recovery","My predecessor previous is-->"+myPredecessor2);
					Log.v("Recovery","My Successor2 is-->"+successor_2);
					SimpleDynamoProvider simplProvidor=new SimpleDynamoProvider();
					try
					{
						Socket skt_successor = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(successor_1));
						PrintWriter pw = new PrintWriter(new OutputStreamWriter(skt_successor.getOutputStream()));
	//9021463990				
	//requestFailedVals.append("~~");
						requestFailedVals.append("#MyPred");
						requestFailedVals.append(myPredeccesor);
						requestFailedVals.append("\n");
						pw.write(requestFailedVals.toString());  
						pw.flush();
									
												
						BufferedReader buff= new BufferedReader(new InputStreamReader(skt_successor.getInputStream()));
						String line = buff.readLine();
						Log.v("Recovery from successor-->","From Sucessor-->"+successor_1);
						//Log.v("Recovery from sucessor1 "+successor_1,"Values are"+line);
						String valueGot[]=line.split("\n");
						String v=simplProvidor.insertValuesRetrieved(line);
						
						pw.close();
						skt_successor.close(); 
					}
					catch(Exception e)
					{
						e.printStackTrace();
					}
					
					try
					{
						Socket skt_successor2 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(successor_2));
						PrintWriter pws2 = new PrintWriter(new OutputStreamWriter(skt_successor2.getOutputStream()));
						//requestFailedVals.append("~~");
						
						pws2.write(requestFailedVals.toString());  
						pws2.flush();
									
												
						BufferedReader buff= new BufferedReader(new InputStreamReader(skt_successor2.getInputStream()));
						String line = buff.readLine();
						Log.v("Recovery from successor2-->","From Sucessor-->"+successor_2);
						//Log.v("Recovery from sucessor2-->"+successor_2,"Values are"+line);
						
						String v=simplProvidor.insertValuesRetrieved(line);
						
						pws2.close();
						skt_successor2.close(); 
					}
					catch(Exception e)
					{
						e.printStackTrace();
					}
					
					
					try
					{
						Socket skt_predecessor = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(myPredeccesor));
						PrintWriter pw_1 = new PrintWriter(new OutputStreamWriter(skt_predecessor.getOutputStream()));
						//requestFailedToPredecessor.append("~~");
						 requestFailedToPredecessor.append("\n");
						 pw_1.write(requestFailedToPredecessor.toString());  
						 pw_1.flush();
						 
						 BufferedReader buff= new BufferedReader(new InputStreamReader(skt_predecessor.getInputStream()));
						 String line = buff.readLine();
						 
						 String v=simplProvidor.insertValuesRetrieved(line); 
						 Log.v("Recovery","From Predecessor-1->"+myPredeccesor);
						// Log.v("Recovery first predecessor "+myPredeccesor,"Values are"+line);
						
						 pw_1.close();
						 skt_predecessor.close();
					 
					}
					catch(Exception e)
					{
						e.printStackTrace();
					}
					 
					try
					{
						 Socket skt_predecessor2 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(myPredecessor2));
						 PrintWriter pw_2 = new PrintWriter(new OutputStreamWriter(skt_predecessor2.getOutputStream()));
						 pw_2.write(requestFailedToPredecessor.toString());  
						 pw_2.flush();
						 
						 BufferedReader buff= new BufferedReader(new InputStreamReader(skt_predecessor2.getInputStream()));
						 String line = buff.readLine();
						 Log.v("Recovery","From Predecessor2-->"+myPredecessor2);
						 //Log.v("Recovery from Predecessor2-->"+myPredecessor2,"Values are"+line);
						 String v=simplProvidor.insertValuesRetrieved(line); 
						 pw_2.close();
						 skt_predecessor2.close();
						 
					}
					catch(Exception e)
					{
						e.printStackTrace();
					}
					
					SimpleDynamoProvider.QueryDoFlag=false;
					
				}
				
			}
		
			
			
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		return null;
	}

}
