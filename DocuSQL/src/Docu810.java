import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.io.BufferedOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.util.*;
import javax.swing.JOptionPane;

public class Docu810 {
	static char FS_ASCHII = 28;
	static int count = 0;
	static ArrayList<String> arrayList = new ArrayList<>();
	static StringBuilder sb = new StringBuilder();

	  private static String segment(String value) { 
		  if(value != null) {
			  count++;		
			  sb.append(value);
			  sb.append(FS_ASCHII);
			  return value;
			  
		  }else {
			  return ""; 
		  }
		
		  }
	 
	public static void main(String[] args) throws FileNotFoundException{
		BufferedOutputStream Docu810 = null;
		String Filename = "Document810_";
		
		try {
			Class.forName("com.mysql.jdbc.Driver");
			Connection con = DriverManager.getConnection("jdbc:mysql://52.202.240.75:3306/sewonedi","wenestimadm","wenestimadm38$!");
		
			String header_array = "select ST02 from sewonedi.header_f810;";
			Statement header_st = con.createStatement();

			ResultSet rs_header = header_st.executeQuery(header_array);

			while (rs_header.next()) {
				String obj = rs_header.getString("ST02");
				arrayList.add(obj);
			}
			rs_header.close();
			
			for(int i=0;i< arrayList.size();i++) {
				System.out.println(i);
				count=0;
				sb.setLength(0);
				
				String EXIST = "SELECT EXISTS (select * from detail_f810 where ST02='";
				EXIST +=arrayList.get(i);
				EXIST +="')as success;";
				
				Statement EX = con.createStatement();
				ResultSet rs_EX =EX.executeQuery(EXIST);
				
				while(rs_EX.next()) {  //Validation
					if(rs_EX.getInt("success")==0) {
						String MSG="Document ST02 ";
						MSG +=arrayList.get(i);
						MSG +=" Creation failed";
						JOptionPane.showMessageDialog(null,MSG);
					}else {
		
						String filename_num = Filename.concat(arrayList.get(i));
						filename_num = filename_num.concat(".txt");
						System.out.println(filename_num);
						
						Docu810 = new BufferedOutputStream(new FileOutputStream(filename_num));
						
						String header_810="";
						header_810+="SELECT concat_WS('~','ST',ST01,ST02)AS ST, \n";
						header_810+="concat('BIG','~',BIG01,'~',BIG02,'~','~~~~',BIG07)AS BIG, \n";
						header_810+="concat_WS('~','CUR',CUR01,CUR02)AS CUR, \n";
						header_810+="concat('REF','~','CA','~',CA_REF02)AS CA_REF, \n";
						header_810+="concat('REF','~','DP','~',DP_REF02)AS DP_REF, \n";
						header_810+="concat('REF','~','PE','~',PE_REF02)AS PE_REF, \n";
						header_810+="concat('N1','~','BT','~',BT_N102,'~',BT_N103,BT_N104)AS BT_N1, \n";
						header_810+="concat('N1','~','ST','~',ST_N102,'~',ST_N103,ST_N104)AS ST_N1, \n";
						header_810+="concat('N1','~','SU','~',SU_N102,'~',SU_N103,SU_N104)AS SU_N1, \n";
						header_810+="concat('DTM','~',DTM01,'~',DTM02)AS DTM, \n";
						header_810+="concat_WS('~','TDS',TRUNCATE(TDS01,0))AS TDS \n";
						header_810+=" from header_f810 ";
						header_810+=" WHERE ST02='";
						header_810+=arrayList.get(i);
						header_810+="';";
						
						System.out.println(header_810);

						Statement st = con.createStatement();
						ResultSet rs = st.executeQuery(header_810);
						
						
						String qu_detail="";
						qu_detail+="SELECT concat('IT1','~',IT101,'~',IT102,'~',IT103,'~',IT104,'~~',IT106,IT107,IT108,IT109)AS IT1, \n";
						qu_detail+="concat('REF','~','PK','~',PK_REF02)AS PK_REF, \n";
						qu_detail+="concat('ITA','~',ITA01,'~~','~',ITA04,'~',ITA05,'~~',TRUNCATE(ITA07,0))AS ITA \n";
						qu_detail+=" from detail_f810 ";
						qu_detail+="WHERE ST02='";
						qu_detail+=arrayList.get(i);
						qu_detail+="';";
						System.out.println(qu_detail);
						
						Statement st2=con.createStatement();
						ResultSet rs2 = st2.executeQuery(qu_detail);
						
						while(rs.next()) {
							segment(rs.getString("ST"));
							segment(rs.getString("BIG"));
							segment(rs.getString("CUR"));
							segment(rs.getString("SU_N1"));
							segment(rs.getString("ST_N1"));
							segment(rs.getString("CA_REF"));
							segment(rs.getString("DP_REF"));
							segment(rs.getString("PE_REF"));
							segment(rs.getString("PE_REF"));
							segment(rs.getString("BT_N1"));
							segment(rs.getString("DTM"));			
							
							while(rs2.next()) {
								System.out.println("RS2");
								segment(rs2.getString("IT1"));
								segment(rs2.getString("PK_REF"));
								segment(rs2.getString("ITA"));
							}
							segment(rs.getString("TDS"));
						}//rs1
						
						Statement st3 = con.createStatement();				
						
						String CTT = "";
						CTT+="select count(ITA01) from detail_f810;";
						
						ResultSet rs3 = st3.executeQuery(CTT);
						
						while(rs3.next()) {
							System.out.println("RS3");
							sb.append(("CTT~"));
							segment(rs3.getString("count(ITA01)"));
							 count++;
						  }//rs3
						  st3.close();
						 sb.append(("SE~"));
						 count++;
						  sb.append(Integer.toString(count));
						  sb.append("~");
						  sb.append(arrayList.get(i));
						  sb.append(FS_ASCHII);
			
						Docu810.write(sb.toString().getBytes());
						Docu810.close();
					}
				}
				

				}//for
			  
			System.out.println("DONE");
		}catch(Exception e) {
			e.getStackTrace();
		}finally {
		}

	}

}
