package occupancy;

import java.nio.DoubleBuffer;
import java.nio.channels.NonWritableChannelException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import javax.naming.InitialContext;
import javax.net.ssl.SSLContext;

import org.omg.CORBA.PUBLIC_MEMBER;

import com.mysql.cj.LicenseConfiguration;
import com.mysql.cj.xdevapi.ColumnDefinition.StaticColumnDefinition;

public class occupancy {
	public static final String URL = "jdbc:mysql://sensoria-mysql.ics.uci.edu:3306/tippersdb_restored?autoReconnect=true&useSSL=false&user=tippersUser&password=tippers2018";

	public static Connection connection;
	public static ResultSet rs;
	
	final static int LengthOfLearnedData = 10;
	public static String queryTime;
	public static String queryRoom;
	
	static class Devices{
		String userId;
		Double confidence;
	}
	
	static class Moment{
		String startTime;
		String endTime;
		String location;
		Double confidence;
	}
	
	static class TransformedMoment{
		String startTime;
		String endTime;
		List<String> location = new ArrayList<>();
		List<Double> confidence = new ArrayList<>(); 
	}
	
	static class Oneday{
		List<Moment> moments = new ArrayList<>();
	}
	
	public static class TransformedOneday{
		List<TransformedMoment> moments = new ArrayList<>();
	}
	
	static class Users{
		String userId;
		List<Oneday> user = new ArrayList<>();
	}
	
	static class TransformedUsers{
		String userId;
		List<TransformedOneday> user = new ArrayList<>();
	}
	
	static class SimilarityValue{
		List<Double> similarity = new ArrayList<>();
	}
	
	public static List<Devices> devices = new ArrayList<>();
	public static List<String> RegisteredDevice = new ArrayList<>();
	public static List<Users> userS = new ArrayList<>();//store unregistered devices
	public static List<TransformedUsers> userST = new ArrayList<>();
	public static List<SimilarityValue> Sim = new ArrayList<>();
	public static int day_th;
	final public static Double eps = 0.01;
	public static Double ExpectedOccupance;
	public static int RegisteredDevices;
	
	public static void main( String[] args ) {
		try {
			 Class.forName("com.mysql.cj.jdbc.Driver");
			 connection = DriverManager.getConnection(URL);
			 System.out.println("Successful connected!");
			 RegisteredDevices=0;
			 ExpectedOccupance=0.0;
			 
			 Init();
			 //read candidate devices
			 ReadQuery(1, queryTime, "", queryRoom, "");
			 //filter registered devices
			 ReadQuery(2, "", "", "", "");
			 CountExpectedOccupance();
			 System.out.println("# of connected devices: "+devices.size());
			 //learn similarity for each unregistered device
			 for(int i=0;i<devices.size();i++) {
				 if(!RegisteredDevice.contains(devices.get(i).userId)) {// not registered
					 ReadQuery(3, queryTime, "", "", devices.get(i).userId);
				 }
				 else {
					 RegisteredDevices ++;
				 }
			 }
			
			 TransformData();
			 //Test(4);
			 ER();
			 System.out.println("# of users: "+ExpectedOccupance);
			 
			 connection.close();
			} 
			catch (ClassNotFoundException e) { e.printStackTrace(); } 
			catch(SQLException e) { e.printStackTrace(); }
	}
	
	public static void Init() {
		queryTime = "2019-01-10 11:00:00";
		queryRoom = "2099";
	}
	
	public static void CountExpectedOccupance() {
		for(int i=0;i<devices.size();i++) {
			ExpectedOccupance += devices.get(i).confidence;
		}
	}
	
	public static String CreateQuery(int queryType, String startTimestamp, String endTimestamp, String location, String userId) {
		String sql = "";
		if (queryType == 1) {//search presence table to find candidate devices 
			StringBuilder query = new StringBuilder();
			String startTimeR=AddDate(1,startTimestamp,3);
			String endTimeTemp = AddDate(1, startTimestamp, 5);
			query.append("select  userID, confidence\n" + 
					"from PRESENCE\n" + 
					"where PRESENCE.startTimestamp >=").append("'"+startTimestamp+"'").append(
					" and PRESENCE.startTimestamp<=").append("'"+startTimeR+"'").append(" and PRESENCE.location=").append(
					"'"+location+"'").append(" and PRESENCE.endTimestamp<=").append("'"+endTimeTemp+"'"); 
			sql = query.toString();
		}
		else if(queryType == 2) {//read registered devices
			StringBuilder query = new StringBuilder();
			query.append("select SEMANTIC_ENTITY_ID from USER where email is not null"); 
			sql = query.toString();
		}
		else if(queryType==3) {//read daily trajectories
			StringBuilder query = new StringBuilder();
			String startTimeR = AddDate(2, startTimestamp, 12);
			query.append("select startTimestamp, endTimestamp, location, confidence from PRESENCE where ").append(
				"userID=").append("'"+userId+"'").append(" and startTimestamp >=").append("'"+startTimestamp+"'").append(
					" and startTimestamp<=").append("'"+startTimeR+"'").append(
				" and endTimestamp<=").append("'"+endTimestamp+"'"); 
			sql = query.toString();
		}
		return sql;
	}
	
	public static void ReadQuery(int queryType, String startTimestamp, String endTimestamp, String location, String userId) {
		if(queryType==1) {
			String sql = CreateQuery(queryType, startTimestamp, endTimestamp, location, userId);
			try {
				Statement stmt = connection.createStatement();
				rs = stmt.executeQuery(sql);
				while(rs.next()) {
					Devices device = new Devices();
					device.userId = rs.getString(1);
					device.confidence = rs.getDouble(2);
					devices.add(device);
				}
			    rs.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		else if(queryType ==2) {
			String sql = CreateQuery(queryType, " ", " ", " ","");
			try {
				Statement stmt = connection.createStatement();
				rs = stmt.executeQuery(sql);
				while(rs.next()) {
					RegisteredDevice.add(rs.getString(1));
				}
			    rs.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		else if(queryType==3) {//read one month day for one user not test
			Users users = new Users();
			users.userId = userId;
			for(int i=0;i<LengthOfLearnedData;i++) {//read one user
				Oneday oneday = new Oneday();
				//System.out.println("** "+oneday.moments.size());
				String startTime = AddDate(3, TransformEarly(startTimestamp), -i);
				String endTime = AddDate(3, TransformLate(startTimestamp), -i);
				String sql = CreateQuery(queryType, startTime, endTime, location, userId);
				//System.out.println(sql);
				try {//read one day
					Statement stmt = connection.createStatement();
					rs = stmt.executeQuery(sql);
					//int count=0;
					while(rs.next()) {
						Moment moment = new Moment();
						moment.startTime = rs.getString(1);
						moment.endTime = rs.getString(2);
						moment.location = rs.getString(3);
						moment.confidence = rs.getDouble(4);
						oneday.moments.add(moment);
						//count++;
					}
				    rs.close();
				    //System.out.println("count= "+count+" "+oneday.moments.size());
				    
				} catch (SQLException e) {
					e.printStackTrace();
				}
				
				users.user.add(oneday);
				
			}
			userS.add(users);
		}
	}
	
	public static Double Similarity(int m,int n) {//similarity between user m and user n 
		Double similarity = 0.0;
		for(int k=0;k<userST.get(m).user.size();k++) {//enumerate days over one month
			Double value = 0.0;
			Double sizem = 0.0;
			Double sizen = 0.0;
			
			for(int i=0;i<userST.get(m).user.get(k).moments.size();i++) {//user m, k-th day
				for(int j=0;j<userST.get(n).user.get(k).moments.size();j++) {//user n, k-th day
					sizem+=getDifference(userST.get(m).user.get(k).moments.get(i).startTime, userST.get(m).user.get(k).moments.get(i).endTime);
					sizen+=getDifference(userST.get(n).user.get(k).moments.get(j).startTime, userST.get(n).user.get(k).moments.get(j).endTime);
				}
			}
			
			for(int i=0;i<userST.get(m).user.get(k).moments.size();i++) {//user m, k-th day
				for(int j=0;j<userST.get(n).user.get(k).moments.size();j++) {//user n, k-th day
					String LI = userST.get(m).user.get(k).moments.get(i).startTime;
					String RI = userST.get(m).user.get(k).moments.get(i).endTime;
					String LJ = userST.get(n).user.get(k).moments.get(j).startTime;
					String RJ = userST.get(n).user.get(k).moments.get(j).endTime;
					if(LJ.compareTo(RI)>0) {
						break;
					}
					else if(RJ.compareTo(LI)<0){
						continue;
					}
					else {
						int DifTime = Math.max(Math.abs(getDifference(RI, LJ)), Math.abs(getDifference(LI, RJ)));
						value += DifTime*CommonRoom(m,n,k,i,j);
					}
				}
			}
			if(sizem == 0 || sizen == 0) continue;
			else {
				similarity += value/Math.min(sizem, sizen);
			}
			
		}
		return similarity;
	}
	
	public static Double CommonRoom(int m, int n, int k,int i, int j) {
		double value = 0.0;
		for(int ii=0;ii<userST.get(m).user.get(k).moments.get(i).location.size();ii++) {
			String location = userST.get(m).user.get(k).moments.get(i).location.get(ii);
			if(userST.get(n).user.get(k).moments.get(j).location.contains(location)) {
				int index = userST.get(n).user.get(k).moments.get(j).location.indexOf(location);
				value = userST.get(m).user.get(k).moments.get(i).confidence.get(ii)+userST.get(n).user.get(k).moments.get(j).confidence.get(index);
			}
		}
		return value;
	}
	
	public static void ER() {
		int entityNum = 0;
		for(int i=0;i<userS.size();i++) {
			SimilarityValue similarityValue = new SimilarityValue();
			for(int j=i+1;j<userS.size();j++) {
				similarityValue.similarity.add(Similarity(i, j));
			}
			Sim.add(similarityValue);
		}
		ExpectedOccupance = Math.ceil(ExpectedOccupance);
	}
	
	
	public static void TransformData() {//transform data: discrete 
		for(int id=0;id<userS.size();id++) {//for each user
			TransformedUsers users = new TransformedUsers();
			users.userId = userS.get(id).userId;
			TransformedMoment moment = null;
			for(int k=0;k<userS.get(id).user.size();k++) {//for each day
				TransformedOneday oneday = new TransformedOneday();
				
				for(int i=0;i<userS.get(id).user.get(k).moments.size();i++) {//moments in one day
					if(i==0 || userS.get(id).user.get(k).moments.get(i).startTime.compareTo(userS.get(id).user.get(k).moments.get(i-1).startTime)!=0) {
						if(i==0) {
							moment = new TransformedMoment();
							moment.startTime=userS.get(id).user.get(k).moments.get(0).startTime;
							moment.endTime=userS.get(id).user.get(k).moments.get(0).endTime;
							moment.confidence.add(userS.get(id).user.get(k).moments.get(0).confidence);
							moment.location.add(userS.get(id).user.get(k).moments.get(0).location);
						}
						else if(userS.get(id).user.get(k).moments.get(i).startTime.compareTo(userS.get(id).user.get(k).moments.get(i-1).startTime)!=0) {
							oneday.moments.add(moment);
							//initialize moments
							moment = new TransformedMoment();
							moment.startTime=userS.get(id).user.get(k).moments.get(i).startTime;
							moment.endTime=userS.get(id).user.get(k).moments.get(i).endTime;
							moment.confidence.clear();
							moment.location.clear();
							moment.confidence.add(userS.get(id).user.get(k).moments.get(i).confidence);
							moment.location.add(userS.get(id).user.get(k).moments.get(i).location);
						}
					}
					else if(userS.get(id).user.get(k).moments.get(i).startTime.compareTo(userS.get(id).user.get(k).moments.get(i-1).startTime)==0) {
						moment.confidence.add(userS.get(id).user.get(k).moments.get(i).confidence);
						moment.location.add(userS.get(id).user.get(k).moments.get(i).location);
						if(i==userS.get(id).user.get(k).moments.size()-1) {//update
							oneday.moments.add(moment);
						}
					}
					
					
				}
				users.user.add(oneday);
				
				//System.out.println("### " + users.user.get(0).moments.get(0).startTime);
			}
			userST.add(users);
		}
	}
	
	
	public static int getDifference(String fromDate, String toDate) {
		SimpleDateFormat simpleFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		int seconds = 0;
		try {
			long from = simpleFormat.parse(fromDate).getTime();
			long to = simpleFormat.parse(toDate).getTime();
			seconds = (int) ((to - from) / 1000);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return seconds;
	}
	
	public static String TransformEarly(String time) {
		return time.substring(0, 11)+"08:00:00";
	}
	
	public static String TransformLate(String time) {
		return time.substring(0, 11)+"20:00:00";
	}
	
	public static String AddDate(Integer type, String date, int offset) {
		Date clock = new Date();
		SimpleDateFormat dataformat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		try {
			clock = dataformat.parse(date);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(clock);
		if(type == 1) {
			calendar.add(Calendar.MINUTE, offset);
		}
		else if (type==2) {
			calendar.add(Calendar.HOUR, offset);
		}
		else if (type==3) {
			calendar.add(Calendar.DATE, offset);
		}
		Date m = calendar.getTime();
		String newDate = dataformat.format(m);

		return newDate;
	}
	
	public static void Test(int type) {
		if(type==1) {
			System.out.println(devices.size());
			for(int i=0;i<devices.size();i++) {
				System.out.println(devices.get(i).userId + " " + devices.get(i).confidence);
			}
		}
		if(type==2) {
			System.out.println(userS.size());
			for(int i=0;i<userS.size();i++) {
				System.out.println(userS.get(i).userId+" "+userS.get(i).user.size());
				for(int j=0;j<userS.get(i).user.size();j++) {
					System.out.println(userS.get(i).user.get(j).moments.size());
					for(int k=0;k<userS.get(i).user.get(j).moments.size();k++) {
						System.out.println(userS.get(i).user.get(j).moments.get(k).startTime + " " + userS.get(i).user.get(j).moments.get(k).endTime + " "+ userS.get(i).user.get(j).moments.get(k).location + " " + userS.get(i).user.get(j).moments.get(k).confidence);
					}
				}
			}
		}
		if(type==3) {
			System.out.println(userST.size());
			for(int i=0;i<userST.size();i++) {
				System.out.println(userST.get(i).userId+" "+userST.get(i).user.size());
				for(int j=0;j<userST.get(i).user.size();j++) {
					System.out.println(userST.get(i).user.get(j).moments.size());
					for(int k=0;k<userST.get(i).user.get(j).moments.size();k++) {
						System.out.println(userST.get(i).user.get(j).moments.get(k).startTime+" "+userST.get(i).user.get(j).moments.get(k).endTime);
						for(int p=0;p<userST.get(i).user.get(j).moments.get(k).location.size();p++) {
							System.out.println(userST.get(i).user.get(j).moments.get(k).location.get(p) + " " + userST.get(i).user.get(j).moments.get(k).confidence.get(p));
						}
					}
				}
			}
		}
		if(type==4) {
			System.out.println(userS.size());
			for(int i=0;i<userS.size();i++) {
				for(int j=i+1;j<userS.size();j++) {
					System.out.println(Similarity(i, j));
				}
			}
		}
	}
}

