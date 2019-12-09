import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.ArrayList;
import java.io.BufferedInputStream;



public class Document856 {
	static String ST_02;
	static String delimeter = "";
	static int blankCount = 0;
	static ArrayList<String> arrayList = new ArrayList<>();
	static ArrayList<String> NHeaderList = new ArrayList<>();
	static ArrayList<String> HeaderList = new ArrayList<>();
	
	private static void ST_02(String string) {
		ST_02 = string;		
	}
	private static void N1NullCheck(String field,String header) {
		if(field != null) {
			NHeaderList.add(header);
			
		}else if(field == null) {
			NHeaderList.add("null");
		}
	}//N1NullCheck
	private static int countLines()throws IOException{
		InputStream is = new BufferedInputStream(new FileInputStream("test.txt"));
				try {
					byte[] c = new byte[1024];
					int count = 0;
					int readChars = 0;
					boolean empty = true;
					while((readChars = is.read(c)) != -1) {
						empty = false;
						for(int i=0;i<readChars;++i) {
							if(c[i] == '\n') {
								++count;
							}
						}
					}
					System.out.println(count);
					return (count ==0 && !empty) ? 1: count;
				}finally {
					
					is.close();
				}
				
	}//countLines()
	private static void NullArrayCheck(String field,String header) {
		 if(field != null){ 
			 arrayList.add(field);
			 HeaderList.add(header);
		 }else if(field == null){
			 arrayList.add("null");
			 HeaderList.add("null");
		 }
	}//NullArrayCheck
	
	
	@SuppressWarnings("null")
	private static String Nullcheck(String value) {
		if (value ==null) {
			return "";
		}else {
			return value;
		}

	}

	public static void main(String[] args) {
		String LIN_06;
		File file = new File("test.txt");
		FileWriter writer = null;

		try {
			Class.forName("com.mysql.jdbc.Driver");
			Connection con = DriverManager.getConnection("jdbc:mysql://52.202.240.75:3306/sewonedi", "wenestimadm","wenestimadm38$!");

			String qu = "select * from header_f856;";
			String qu2 = "select * from shipment_f856;"; 
			String qu3 = "select * from order_f856;";
			String qu4 = "select O_HL01 from sewonedi.order_f856 order by O_HL01 desc limit 1;";
			
			Statement st = con.createStatement();
			Statement st2 = con.createStatement();
			Statement st3 = con.createStatement();
			Statement st4 = con.createStatement();
			
			ResultSet rs = st.executeQuery(qu);
			ResultSet rs2 = st2.executeQuery(qu2);
			ResultSet rs3 = st3.executeQuery(qu3);
			ResultSet rs4 = st4.executeQuery(qu4);

			writer = new FileWriter(file, false);
			while (rs.next()) {		
				StringJoiner sj = new StringJoiner("~");
				sj.add("ST");
				sj.add(Nullcheck(rs.getString("ST01")));
				sj.add(Nullcheck(rs.getString("ST02")));
				String ST = sj.toString();
				writer.write(ST);
				writer.write("\n");		
				
				StringJoiner BSN = new StringJoiner("~");
				BSN.add("BSN"); BSN.add(Nullcheck(rs.getString("BSN01")));BSN.add(Nullcheck(rs.getString("BSN02")));BSN.add(Nullcheck(rs.getString("BSN03")));BSN.add(Nullcheck(rs.getString("BSN04")));
								
				String BSNSet = BSN.toString();
				writer.write(BSNSet);
				writer.write("\n");	

				StringJoiner DTM = new StringJoiner("~");
				DTM.add("DTM");DTM.add(Nullcheck(rs.getString("DTM01")));DTM.add(Nullcheck(rs.getString("DTM02")));DTM.add(Nullcheck(rs.getString("DTM03")));DTM.add(Nullcheck(rs.getString("DTM04")));
				String DTMSet = DTM.toString();
				writer.write(DTMSet);
				writer.write("\n");	
				writer.write("\n");
	
				writer.flush();
				System.out.println("Tabel 1 DONE!");
			} // while()
			st.close();
			blankCount+=1;
			
			while (rs2.next()) { // second Table
				StringJoiner HL_O = new StringJoiner("~");
				HL_O.add("HL");HL_O.add(Nullcheck(rs2.getString("S_HL01")));HL_O.add(delimeter);HL_O.add(Nullcheck(rs2.getString("S_HL03")));
				String HL_OSet = HL_O.toString();
				writer.write(HL_OSet);				
				writer.write("\n");
				
				StringJoiner G_MEA = new StringJoiner("~");
				G_MEA.add("MEA");G_MEA.add(Nullcheck(rs2.getString("G_MEA01")));G_MEA.add("G");G_MEA.add(Nullcheck(rs2.getString("G_MEA03")));G_MEA.add(Nullcheck(rs2.getString("G_MEA04")));
				String G_MEASet = G_MEA.toString();
				writer.write(G_MEASet);
				
				StringJoiner N_MEA = new StringJoiner("~");
				N_MEA.add("MEA");N_MEA.add(Nullcheck(rs2.getString("N_MEA01")));N_MEA.add("N");N_MEA.add(Nullcheck(rs2.getString("N_MEA03")));N_MEA.add(Nullcheck(rs2.getString("N_MEA04")));
				writer.write("\n");
				String N_MEASet = N_MEA.toString();
				writer.write(N_MEASet);
				writer.write("\n");
				
				StringJoiner TD1 = new StringJoiner("~");
				TD1.add("TD1");TD1.add(Nullcheck(rs2.getString("TD101")));TD1.add(Nullcheck(rs2.getString("TD102")));
				String TD1Set =TD1.toString();
				writer.write(TD1Set);
				writer.write("\n");
	
				StringJoiner TD5 = new StringJoiner("~");
				TD5.add("TD5");
				TD5.add(Nullcheck(rs2.getString("TD501")));
				TD5.add(Nullcheck(rs2.getString("TD502")));
				TD5.add(Nullcheck(rs2.getString("TD503")));
				TD5.add(Nullcheck(rs2.getString("TD504")));
				if(rs2.getString("TD505")==null && rs2.getString("TD507")==null && rs2.getString("TD508")==null) {
					System.out.println("Three null");
					writer.write(TD5.toString());
					System.out.println(TD5.toString());
					writer.write("\n");
				}else {
					TD5.add(Nullcheck(rs2.getString("TD505")));
					TD5.add(delimeter);TD5.add(Nullcheck(rs2.getString("TD507")));TD5.add(Nullcheck(rs2.getString("TD508")));
					writer.write(TD5.toString());
					writer.write("\n");

				}
			

				StringJoiner TD3 = new StringJoiner("~");
				TD3.add("TD3");TD3.add(Nullcheck(rs2.getString("TD301")));TD3.add(Nullcheck(rs2.getString("TD302")));TD3.add(Nullcheck(rs2.getString("TD303")));
				String TD3Set = TD3.toString();
				writer.write(TD3Set);
				writer.write("\n");

				  NullArrayCheck(rs2.getString("AO_REF02"),"AO");
				  NullArrayCheck(rs2.getString("AW_REF02"),"AW");
				  NullArrayCheck(rs2.getString("BM_REF02"),"BM");
				  NullArrayCheck(rs2.getString("CN_REF02"),"CN");
				  NullArrayCheck(rs2.getString("FR_REF02"),"FR");
				  NullArrayCheck(rs2.getString("MB_REF02"),"MB");
				  NullArrayCheck(rs2.getString("PK_REF02"),"PK");
				  NullArrayCheck(rs2.getString("SN_REF02"),"SN");
				  NullArrayCheck(rs2.getString("DK_REF02"),"DK"); 
	
				  for(int i=0;i<arrayList.size();i++) {
					  if(arrayList.get(i).equals("null")) {					 
					  }else {												  
						 writer.write(String.format("%s~%s~%s","REF",HeaderList.get(i),arrayList.get(i)));
						 writer.write("\n");
						  }
					  
					  }
				  N1NullCheck(rs2.getString("ST_N103"),"ST");				
				  N1NullCheck(rs2.getString("SF_N103"),"SF");		
				  N1NullCheck(rs2.getString("SU_N103"),"SU");
				  N1NullCheck(rs2.getString("MA_N103"),"MA");
				 
				for(int i=0;i<NHeaderList.size();i++) {
					if(NHeaderList.get(i).equals("null")) {
					}else {
						String field = NHeaderList.get(i);
						writer.write(String.format("%s~%s~~%s~%s", "N1",field,rs2.getString(field.concat("_N103")),rs2.getString(field.concat("_N104"))));
						writer.write("\n");
					}
					
				}
				writer.write("\n");
				writer.flush();		  
				System.out.println("Tabel 2 DONE!");
			}//while2()
			st2.close();
			blankCount+=1;
			
			while(rs3.next()) {
				StringJoiner O_HL = new StringJoiner("~");
				O_HL.add("HL");O_HL.add(Nullcheck(rs3.getString("O_HL01")));O_HL.add(Nullcheck(rs3.getString("O_HL02")));O_HL.add(Nullcheck(rs3.getString("O_HL03")));				
				writer.write(O_HL.toString());
				writer.write("\n");
				
				StringJoiner LIN = new StringJoiner("~");
				LIN.add("LIN");LIN.add(delimeter);
				System.out.println(rs3.getString("LIN02"));
				if(rs3.getString("LIN02").contentEquals("BP") ||rs3.getString("LIN02").contentEquals("VP") ) {
					System.out.println("BP!");
					LIN.add(Nullcheck(rs3.getString("LIN02")));LIN.add(Nullcheck(rs3.getString("LIN03")));LIN.add(Nullcheck(rs3.getString("LIN04")));LIN.add(Nullcheck(rs3.getString("LIN05")));
					LIN.add("RC");LIN.add(Nullcheck(rs3.getString("LIN07")));
				}else {
					LIN.add("RC");LIN.add(Nullcheck(rs3.getString("LIN03")));
				}
				System.out.println(LIN.toString());
				writer.write(LIN.toString());
				writer.write("\n");
				
				StringJoiner SN1 = new StringJoiner("~");
				SN1.add("SN1");SN1.add(delimeter);SN1.add(Nullcheck(rs3.getString("SN102")));SN1.add(Nullcheck(rs3.getString("SN103")));SN1.add(Nullcheck(rs3.getString("SN104")));
				writer.write(SN1.toString());
				writer.write("\n");
				
				StringJoiner PRF = new StringJoiner("~");
				PRF.add("PRF");PRF.add(rs3.getString("PRF01"));
				writer.write(PRF.toString());
				writer.write("\n");
				
				ST_02(rs3.getString("ST02"));
				writer.flush();	
				
			}//while3()
			
			
			st3.close();
			while(rs4.next()) { 
				writer.write("\n");
				writer.write(String.format("%s~%s","CTT",rs4.getString("O_HL01")));
				writer.flush();
			}
			int lnum = countLines() - blankCount;
			
			writer.write("\n");
			writer.write(String.format("%s~%s~%s","SE",lnum,ST_02));
			writer.flush();	
			
		} catch (Exception e) {
			System.err.println("Got an exception");
			System.err.println(e.getMessage());
		} // catch
	}
	
}
