import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Scanner;


public class DB_Create_Index {

	static String offset_s[]=null;
	static int key_size;
	static String Level="001";
	static String Block="001";
	static String Element="001";
	static	String key=null;
	static char l_p='L';
	static String l_offset="0000000000";
	static String r_offset="0000000000";
	static String cur_offset;
	static int line = 0;
	static int block_size;
	static FileReader fr;
	static RandomAccessFile ra;
	static BufferedReader br;
	static FileReader f1;
	static BufferedReader br1;
	static FileWriter fw;
	static int add;
	static int cur_data_offset;
	
	/*
	 * Module Description: Constructor to initialize variables
	 */
	public DB_Create_Index(int ks, String index, String test_data) throws IOException
	{
		fw=new FileWriter(new File("offset.txt"));
		fr=new FileReader(new File("offset.txt"));
		ra=new RandomAccessFile(index, "rw");
		br=new BufferedReader(fr);
		f1=new FileReader(new File(index));
		br1 = new BufferedReader(f1);
		key_size=ks;
		add=49+ks;
		cur_offset=String.valueOf(test_data.length()+1+String.valueOf(ks).length()+2);
		cur_data_offset=test_data.length()+1+String.valueOf(ks).length()+2;
	}
	
	/*
	 * Module Description: Main Function to call create, find, list or insert
	 */
	@SuppressWarnings({ "unused", "resource" })
	public static void main(String[] args) throws IOException 
	{
			Scanner in=new Scanner(System.in);
			String input=in.nextLine();
			String[] split_dash=input.split("-");
			String[] split_space = split_dash[1].split(" ");
			DB_Create_Index db;
			if(split_space[0].equalsIgnoreCase("create"))
			{
				Path p=Paths.get(split_space[3]);
				Files.deleteIfExists(p);
				db=new DB_Create_Index(Integer.parseInt(split_space[1]),split_space[3],split_space[2]);
			}
			else
			{
				RandomAccessFile d=new RandomAccessFile(split_space[2], "rw");
				String[] key=d.readLine().split("\t");
				db=new DB_Create_Index(Integer.parseInt(key[1]),split_space[2],split_space[1]);
			}
			
			if(split_space[0].equalsIgnoreCase("create"))
			{
	    		get_key_offset(key_size,split_space[0],"",split_space[2]);
	    		RandomAccessFile d=new RandomAccessFile("offset.txt", "rw");
				block_size=(int) (d.length()/(11+key_size));
				block_size=block_size/3;
				if(block_size<4)block_size=100;
				create_index(split_space[0],split_space[2]);
			}

			boolean t;

			if(split_space[0].equalsIgnoreCase("find"))
			{
				RandomAccessFile d=new RandomAccessFile("offset.txt", "rw");
				block_size=(int) (d.length()/(11+key_size));
				block_size=block_size/3;
				if(block_size<4)block_size=100;
				t=FIND(split_space[1],split_space[3],split_space[0]);
			}
				
			if(split_space[0].equalsIgnoreCase("list"))
			{
				RandomAccessFile d=new RandomAccessFile("offset.txt", "rw");
				block_size=(int) (d.length()/(11+key_size));
				block_size=block_size/3;
				if(block_size<4)block_size=100;
				LIST(split_space[1],split_space[3],split_space[4]);
			}
			
			if(split_space[0].equalsIgnoreCase("insert"))
			{
				RandomAccessFile d=new RandomAccessFile("offset.txt", "rw");
				block_size=(int) (d.length()/(11+key_size));
				block_size=block_size/3;
				if(block_size<4)block_size=100;
				String[] rec=split_dash[1].split("\"");
				get_key_offset(key_size,split_space[0],rec[1],split_space[1]);
				String[] split=split_space[0].split(" ");
				if(!FIND(split_space[1],split[0],"insert"))create_index("insert",split_space[1]);
			}
	}
	
	/*
	 * Module Description: List Sequential Records
	 */
	private static void LIST(String input,String key, String count_string) throws IOException 
	{
		 int count=get_dec(count_string);
	     RandomAccessFile frr = new RandomAccessFile(input, "rw");
	     long ptr=traverse(key);
	     ra.seek(ptr);
	     String ss=ra.readLine();
	     String s[]=ss.split(" ");
	     while(ss!=null)
	     {
	    	 //If the record is found, list from there
	        if(s[3].equals(key))
	        {
	           frr.seek(get_dec(s[6]));
	           System.out.println("At "+get_dec(s[6])+", record: "+frr.readLine());
	           for(int i=0;i<count;i++)
	           {
	               ss=ra.readLine();
	               if(ss==null)
	               {
	                  frr.close();
	                  return;
	               }
	               s=ss.split(" ");
	               frr.seek(get_dec(s[6]));
	               System.out.println("At "+get_dec(s[6])+", record: "+frr.readLine());
	           }
	           frr.close();
	           return;
	         }
	        // If not found, go to the next record and list from there
	         else if(s[3].compareTo(key)>0)
	         {
	            System.out.println("Key not found... The next keys are....");
	            frr.seek(get_dec(s[6]));
	            for(int i=0;i<count;i++)
	            {
	               ss=ra.readLine();
	               if(ss==null)
	               {
	                 frr.close();
	                 return;
	               }
	               s=ss.split(" ");
	               frr.seek(get_dec(s[6]));
	               System.out.println("At "+get_dec(s[6])+", record: "+frr.readLine());
	            }
	            frr.close();
	            return;
	         }
	         ss=ra.readLine();
	         if(ss!=null) s=ss.split(" ");
	    }
	    frr.close();
	}
	
	/*
	 * Module Description: Find a record by key
	 */
	
	private static boolean FIND(String input,String key, String command) throws IOException 
	{
		RandomAccessFile frr = new RandomAccessFile(input, "rw");
		
		ra.seek(0);
		String first_line=ra.readLine();
		String[] first_line_key=first_line.split("\t");
			
		//To truncate or pad the key that needs to be inserted
		if(key.length()<Integer.parseInt(first_line_key[1]))
			key=right_padding(key,first_line_key[1]);
		else if(key.length()>Integer.parseInt(first_line_key[1]))
			key=key.substring(0, Integer.parseInt(first_line_key[1]));

		long ptr=traverse(key);
		ra.seek(ptr);
		String ss=ra.readLine();
		String s[]=null;
		if(ss!=null)s=ss.split(" ");
		String block=s[1];
		
		while(ss!=null && s[1].equals(block))
		{
			if(s[3].equals(key))
			{
				frr.seek(get_dec(s[6]));
				if(command.equalsIgnoreCase("find")) System.out.println("At "+get_dec(s[6])+", record: "+frr.readLine());
				return true;
			}
			ss=ra.readLine();
			if(ss!=null) s=ss.split(" ");
		}
		if(command.equalsIgnoreCase("find")) System.out.println("Key Not Found...");
		return false;
	}
	
	/*
	 * Module Description: Called from main function to build the index file.
	 */
	private static void create_index(String command, String testdata) throws IOException 
	{
		RandomAccessFile ra_offset=new RandomAccessFile("offset.txt", "rw");
		
		if(command.equalsIgnoreCase("create"))
		{
			ra_offset.seek(0);
			line=0;
		}
		else if(command.equalsIgnoreCase("insert"))
		{
			int len=(int) ra_offset.length();
			len=len-(11+key_size+2);
			ra_offset.seek(len);
			line=len/(11+key_size+2-1);
		}
		String s=ra_offset.readLine();
		while(s!=null)
		{
			System.out.println("Inserting this....."+s);
			line++;
			offset_s=s.split(",");
			if(ra.length()==0)
			{
				r_offset=offset_s[0];
				ra.writeBytes(testdata+"\t"+key_size+"\r\n");
				cur_offset=get_hex(Integer.parseInt(cur_offset));
				ra.writeBytes(Level+" "+Block+" "+Element+" "+offset_s[1]+" "+l_p+" "+l_offset+" "+r_offset+" "+cur_offset+"\r\n");
				cur_offset=get_next_offset(cur_offset);
			}
			else
				insert_rec(offset_s);
			if(command.equalsIgnoreCase("insert")) System.out.println("Success!!! "+offset_s[1]+" inserted at "+line);
			s=ra_offset.readLine();
			line++;
		}
		ra_offset.close();
	}
 
	
	/*
	 * Module Description: To fetch the next record's offset
	 */
	private static String get_next_offset(String cur_offset) 
	{
		int dec=get_dec(cur_offset);
		cur_offset=get_hex(dec+add);
		cur_offset=left_padding(cur_offset);
		return cur_offset;
	}
	
	/*
	 * Module Description: To convert hex to decimal
	 */
	private static int get_dec(String cur_offset2) 
	{
		int val=Integer.parseInt(cur_offset2,16);
		return val;
	}
	
	/*
	 * Module Description: To convert decimal to hex
	 */
	private static String get_hex(int i) 
	{
		String s=String.valueOf(Integer.toHexString(i));
		s=left_padding(s);
		return s;
	}
	
	/*
	 * Module Description: Called from main function to insert a record into the tree
	 */
	private static void insert_rec(String[] offset_s2) throws IOException 
	{
		long ptr=traverse(offset_s2[1]);
		ra.seek(ptr);
		String s;
		s=ra.readLine();
		String[] s_index;
		int block_count=get_block_count(s);
		s_index=s.split(" ");
		String block=s_index[1];
		ra.seek(ptr);
		int offset = 0;
		if(block_count<block_size)
		{
			while(s!=null)
			{
				s_index=s.split(" ");
				if(s_index[3].compareTo(offset_s2[1])<0 && s_index[1].equals(block))
				{
					offset=get_dec(s_index[7]);
					s=ra.readLine();
				}
				else if(s_index[3].compareTo(offset_s2[1])==0)
				{
					System.out.println(" Duplicate Data "+s_index[3]+", at line "+line);
					return;
				}
				else if(s_index[3].compareTo(offset_s2[1])>0 && s_index[1].equals(block))
				{
					if(Integer.parseInt(s_index[2])<block_size)
					{
						insert(offset_s2,s_index[7],s_index);
						return;
					}
					else
					{
						split(ptr);
						insert_rec(offset_s2);
						
					}
				}
				else break;
			}
			ra.seek(offset);
			String string=ra.readLine();
			String[] prev_str = null;
			if(string!=null)
			{	
				prev_str=string.split(" ");
				if(Integer.parseInt(prev_str[2])<block_size)
				{
					int off=(int) ra.getFilePointer();
					ra.seek(off);
					if(ra.readLine()!=null)
					{
						prev_str[2]=String.format("%03d", Integer.parseInt(prev_str[2])+1);
						insert(offset_s2,get_hex(off),prev_str);
					}
					else
						ra.writeBytes(prev_str[0]+" "+prev_str[1]+" "+String.format("%03d",Integer.parseInt(prev_str[2])+1)+" "+offset_s2[1]+" "+"L"+" "+prev_str[5]+" "+offset_s2[0]+" "+get_hex(off)+"\r\n");
				}
			}
		}
		else
		{
			if(s_index[3].compareTo(offset_s2[1])==0)
			{
				System.out.println(" Duplicate Data "+s_index[3]+", at line "+line);
				return;
			}
			split(ptr);
			insert_rec(offset_s2);			
		}
	}

	/*
	 * Module Description: To fetch the middle element and split the block
	 */
	private static void split(long ptr) throws IOException
	{
		int middle_idx=block_size/2;
		ra.seek(ptr);
		String s=ra.readLine();
		String[] split;
		while(s!=null)
		{
			split = s.split(" ");
			if(Integer.parseInt(split[2])==middle_idx)
			{
				make_middle_root((int) ptr,split[7]);
				return;
			}
				s=ra.readLine();
		}
	}
	
	/*
	 * Module Description: Splits the block and makes the middle node as parent
	 */
	private static void make_middle_root(int ptr, String mptr) throws IOException 
	{
		String[] lchild = get_left_children(ptr,mptr);
		String[] rchild = get_right_children(ptr,mptr);
		int flag=0,i,flagg=0;
		String[] ppp=null;
		String level=null;
		ra.seek(get_dec(mptr));
		String middle_ele=ra.readLine();
		String mid_ele[]=middle_ele.split(" ");
		String[] parents=get_parent_nodes(mid_ele[0]);
		String lkey[] = null,rkey[] = null;
		if(lchild[0]!=null)lkey= lchild[0].split(" ");
		if(rchild[0]!=null)rkey = rchild[0].split(" ");
		//If there is no root
		if(parents[0]==null)
		{
			middle_ele=String.format("%03d",Integer.parseInt(mid_ele[0])+1)+" 001 001 "+mid_ele[3]+" P "+get_hex(get_dec(lkey[7])+add)+" "+get_hex(get_dec(rkey[7])+add)+" "+get_hex(ptr)+"\r\n";
			ra.seek(ptr);
			ra.writeBytes(middle_ele);
			re_write_children(lchild,rchild,ptr,0);
		}
		else
		{
			int block=-1;
			//Traverse through all the parents
			for( i=0;parents[i]!=null && flag==0;i++)
			{
				String[] pp=parents[i].split(" ");
				//If the root is lesser than the middle element, traverse to the next root
				if(pp[3].compareTo(mid_ele[3])<0){}
				else 
				{
					block=get_block_count(parents[i]);
					String blocknew = pp[2];
					if(blocknew.equals("001"))
						{
						    if(i!=0)
						    {
						    	pp=parents[i-1].split(" ");
						    	block=get_block_count(parents[i-1]);
						    	pp[2]=String.format("%03d", Integer.parseInt(pp[2])+1);
						    }
						}
					//If block is not full, proceed to insert
					if(block<block_size)
					{
						String[] ppx=parents[i].split(" ");
						shift_down(get_dec(ppx[7]));
						ra.seek(get_dec(ppx[7]));
						String s=pp[0]+" "+pp[1]+" "+pp[2]+" "+mid_ele[3]+" P "+get_hex(get_dec(lkey[7])+add)+" "+get_hex(get_dec(rkey[7])+add)+" "+ppx[7]+"\r\n";
						ra.writeBytes(s);
						flag=1;
						level=pp[0];
						re_write_children(lchild,rchild,ptr,0);
						change_parents(level,mid_ele[3],0,pp[1]);
					}
					//split the root block
					else
					{
						pp=parents[0].split(" ");
						//shift all the records down, and change its offset
						shift_down(get_dec(get_first_node()));
						split(get_dec(pp[7]));
						String[] p=get_parent_nodes(get_hex(get_dec(pp[0])-1));
						for(int ix=0;p[ix]!=null;ix++)
						{
							ppp=p[ix].split(" ");
							if(ppp[3].compareTo(mid_ele[3])<0){}
							else
							{
								shift_down(get_dec(ppp[7]));
								ra.seek(get_dec(ppp[7]));
								String s=ppp[0]+" "+ppp[1]+" "+ppp[2]+" "+mid_ele[3]+" P "+get_hex(get_dec(lkey[7])+add+add)+" "+get_hex(get_dec(rkey[7])+add+add)+" "+ppp[7]+"\r\n";
								flagg = 1;
								ra.writeBytes(s);
								level=ppp[0];
								lchild = get_left_children(ptr+add+add,get_hex(get_dec(mptr)+add+add));
								rchild = get_right_children(ptr+add+add,get_hex(get_dec(mptr)+add+add));
								re_write_children(lchild,rchild,get_dec(lkey[7])+add,1);
								change_parents(level,mid_ele[3],1,ppp[1]);						
							}
						}
						if(flagg==0)
						{
							shift_down(get_dec(ppp[7])+add);
							ra.seek(get_dec(ppp[7])+add);
							String s=ppp[0]+" "+ppp[1]+" "+ppp[2]+" "+mid_ele[3]+" P "+get_hex(get_dec(lkey[7])+add+add)+" "+get_hex(get_dec(rkey[7])+add+add)+" "+ppp[7]+"\r\n";
							flagg = 1;
							ra.writeBytes(s);
							level=ppp[0];
							lchild = get_left_children(ptr+add+add,get_hex(get_dec(mptr)+add+add));
							rchild = get_right_children(ptr+add+add,get_hex(get_dec(mptr)+add+add));
							re_write_children(lchild,rchild,get_dec(lkey[7])+add,1);
							change_parents(level,mid_ele[3],1,ppp[1]);
						}
						if(flagg==1)
							return;
					}
				}
			}
			if(flag==0)
			{
				
				String[] pp=parents[i-1].split(" ");
				String Block_count=pp[2];
				if(get_dec(Block_count)<block_size)
				{
					shift_down(get_dec(pp[7])+add);
					ra.seek(get_dec(pp[7])+add);
					String s=pp[0]+" "+pp[1]+" "+String.format("%03d",get_dec(pp[2])+1)+" "+mid_ele[3]+" P "+get_hex(get_dec(lkey[7])+add)+" "+get_hex(get_dec(rkey[7])+add)+" "+get_hex(get_dec(pp[7])+add)+"\r\n";
					level=pp[0];
					ra.writeBytes(s);
					re_write_children(lchild,rchild,get_dec(lkey[7]),0);
					change_parents(level,mid_ele[3],0,pp[1]);
					flag=1;
				}
				else
				{
					pp=parents[i-block_size].split(" ");
					split(get_dec(pp[7]));
					String[] p=get_parent_nodes(get_hex(get_dec(pp[0])-1));
					for(int ix=0;p[ix]!=null;ix++)
					{
						ppp=p[ix].split(" ");
						if(ppp[3].compareTo(mid_ele[3])<0){}
						else
						{
							shift_down(get_dec(ppp[7]));
							ra.seek(get_dec(ppp[7]));
							String s=ppp[0]+" "+ppp[1]+" "+ppp[2]+" "+mid_ele[3]+" P "+get_hex(get_dec(lkey[7])+add+add)+" "+get_hex(get_dec(rkey[7])+add+add)+" "+ppp[7]+"\r\n";
							flagg = 1;
							ra.writeBytes(s);
							level=ppp[0];
							lchild = get_left_children(ptr+add+add,get_hex(get_dec(mptr)+add+add));
							rchild = get_right_children(ptr+add+add,get_hex(get_dec(mptr)+add+add));
							re_write_children(lchild,rchild,get_dec(lkey[7])+add,1);
							change_parents(level,mid_ele[3],1,ppp[1]);
						}
					}
					if(flagg==0)
					{
						shift_down(get_dec(ppp[7])+add);
						ra.seek(get_dec(ppp[7])+add);
						ppp[2]=String.format("%03d", get_dec(ppp[2])+1);
						ppp[7]= get_hex(get_dec(ppp[7])+add);
						String s=ppp[0]+" "+ppp[1]+" "+ppp[2]+" "+mid_ele[3]+" P "+get_hex(get_dec(lkey[7])+add+add)+" "+get_hex(get_dec(rkey[7])+add+add)+" "+ppp[7]+"\r\n";
						flagg = 1;
						ra.writeBytes(s);
						level=ppp[0];
						lchild = get_left_children(ptr+add+add,get_hex(get_dec(mptr)+add+add));
						rchild = get_right_children(ptr+add+add,get_hex(get_dec(mptr)+add+add));
						re_write_children(lchild,rchild,get_dec(lkey[7])+add,1);
						change_parents(level,mid_ele[3],1,ppp[1]);
					}
					if(flagg==1)
						return;
				}
			}
			if(flag==1)return;
		}
		
	}
	
	/*
	 * Module Description: To change the leftchild and rightchild offset of the root nodes
	 */
	private static void change_parents(String level,String key,int flagg,String Blo) throws IOException 
	{
		int flag=0;
		ra.seek(cur_data_offset);
		String s=ra.readLine();
		String ss[];
		ss=s.split(" ");
		while(s!=null)
		{
			if(ss[0].equals(level) && !ss[3].equals(key))
			{
				if(ss[3].compareTo(key)>0  && ss[1].equals(Blo))ss[2]=String.format("%03d", get_dec(ss[2])+1);
				if(flagg==0)ss[5]=get_hex(get_dec(ss[5])+add);
				else if(flagg==1)ss[5]=get_hex(get_dec(ss[5])+add+add);
				if(flagg==0) ss[6]=get_hex(get_dec(ss[6])+add);
				else if(flagg==1)ss[6]=get_hex(get_dec(ss[6])+add+add);
				ra.seek(get_dec(ss[7]));
				ra.writeBytes(ss[0]+" "+ss[1]+" "+ss[2]+" "+ss[3]+" "+ss[4]+" "+ss[5]+" "+ss[6]+" "+ss[7]+"\r\n");
				flag=1;
			}
			s=ra.readLine();
			if(s!=null)ss=s.split(" ");
			if(!ss[0].equals(level) && flag==1) break;
		}
	}

	/*
	 * Module Description: To shift all records one position down in the index file
	 */
	private static void shift_down(int get_dec) throws IOException 
	{
		ra.seek(get_dec);
		String r1=ra.readLine();
		String r2=ra.readLine();
		int pos=get_dec+add;
		while(r1!=null || r2!=null)
		{
			ra.seek(pos);
			if(r1!=null)
			{
				String [] r11=r1.split(" ");
				r11[7]=get_hex(get_dec(r11[7])+add);
				if(r11[4].equals("P")) 
				{
					r11[5]=get_hex(get_dec(r11[5])+add);
					r11[6]=get_hex(get_dec(r11[6])+add);
				}
				ra.writeBytes(r11[0]+" "+r11[1]+" "+r11[2]+" "+r11[3]+" "+r11[4]+" "+r11[5]+" "+r11[6]+" "+r11[7]+"\r\n");
			}
			r1=ra.readLine();
			ra.seek(pos+add);
			if(r2!=null)
			{
				String [] r11=r2.split(" ");
				r11[7]=get_hex(get_dec(r11[7])+add);
				if(r11[4].equals("P")) 
				{
					r11[5]=get_hex(get_dec(r11[5])+add);
					r11[6]=get_hex(get_dec(r11[6])+add);
				}
				ra.writeBytes(r11[0]+" "+r11[1]+" "+r11[2]+" "+r11[3]+" "+r11[4]+" "+r11[5]+" "+r11[6]+" "+r11[7]+"\r\n");
			}
			r2=ra.readLine();
			pos=pos+add+add;
		}
		
	}
	
	/*
	 * Module Description: Modify the child records of the new root.
	 */
	private static void re_write_children(String[] lchild, String[] rchild,	int ptr, int flagg) throws IOException 
	{
		String seek=null;
		String s[] = null,s1[];
		ra.seek(ptr);
		String[] ss=ra.readLine().split(" ");
		String level=ss[0];
		String block=ss[1];
		ra.seek(ptr+add);
		for(int i=0;i<lchild.length && lchild[i]!=null ;i++)
		{
			s=lchild[i].split(" ");
			if(s[0].equals(level))
			{
				s[1]=String.format("%03d", Integer.parseInt(block)+1);
			}
			if(flagg==0)s[7]=get_hex(get_dec(s[7])+add);
			ra.writeBytes(s[0]+" "+s[1]+" "+s[2]+" "+s[3]+" "+s[4]+" "+s[5]+" "+s[6]+" "+s[7]+"\r\n");
		}
		for(int i=0;i<rchild.length && rchild[i]!=null ;i++)
		{
			s=rchild[i].split(" ");
			s[1]=String.format("%03d", Integer.parseInt(s[1])+1);
			s[2]=String.format("%03d", i+1)	;
			if(flagg==0)s[7]=get_hex(get_dec(s[7])+add);
			seek=s[7];
			ra.writeBytes(s[0]+" "+s[1]+" "+s[2]+" "+s[3]+" "+s[4]+" "+s[5]+" "+s[6]+" "+s[7]+"\r\n");
		}
		level=s[0];
		String s_next=ra.readLine();
		if(s_next!=null)s=s_next.split(" ");
		while(s_next!=null && s[0].equals(level))
		{
			s=s_next.split(" ");
			s[1]=String.format("%03d", Integer.parseInt(s[1])+1);
			ra.seek(get_dec(s[7]));
			ra.writeBytes(s[0]+" "+s[1]+" "+s[2]+" "+s[3]+" "+s[4]+" "+s[5]+" "+s[6]+" "+s[7]+"\r\n");
			s_next=ra.readLine();
		}
	}
	
	/*
	 * Module Description: To get all the root node at a level
	 */
	private static String[] get_parent_nodes(String level) throws IOException 
	{
		String[] parents=new String[block_size*10];
		int p_level=Integer.parseInt(level)+1;
		ra.seek(cur_data_offset);
		int i=0;
		String s=ra.readLine();
		while(s!=null)
		{
			String[] s_split=s.split(" ");
			if(get_dec(s_split[0])==p_level)
				parents[i++]=s;
			s=ra.readLine();
		}
		return parents;
	}
	
	/*
	 * Module Description: To get all the records in the right side of a root
	 */
	private static String[] get_right_children(long ptr, String mptr) throws IOException 
	{
		ra.seek(get_dec(mptr));
		String[] rchild = new String[block_size*10];
		int i=0;
		String s=ra.readLine();
		String[] aa=s.split(" ");
		int level = Integer.parseInt(aa[0]);
		int block = Integer.parseInt(aa[1]);
		while(Integer.parseInt(aa[1])==block && Integer.parseInt(aa[0])==level && s!=null)
		{
			rchild[i++] = s;
			s=ra.readLine();
			if(s!=null) aa=s.split(" ");
		}
		return rchild;
	}
	
	/*
	 * Module Description: To get all the records in the left side of a root
	 */
	private static String[] get_left_children(long ptr, String mptr) throws IOException 
	{
		String[] lchild = new String[block_size*10];
		String s;
		int i=0;
		while(ptr<get_dec(mptr))
		{
			ra.seek(ptr);
			s=ra.readLine();
			lchild[i++]=s;
			ptr=ptr+add;
		}
		return lchild;
	}

	/*
	 * Module Description: To fetch the number of elements in a block
	 */
	private static int get_block_count(String s) throws IOException 
	{
		int count = 0,block;
		String[] aa=s.split(" ");
		int level=Integer.parseInt(aa[0]);
		block=Integer.parseInt(aa[1]);
		ra.seek(get_dec(aa[7]));
		while(s!=null && Integer.parseInt(aa[1])==block && Integer.parseInt(aa[0])==level)
		{	
			count=Integer.parseInt(aa[2]);
			s=ra.readLine();
			if(s!=null)
				aa=s.split(" ");
		}
		return count;
	}

	/*
	 * Module Description: To insert new record into the index file and shift the following lines one step down
	 */
	private static void insert(String[] offset_s2, String offset, String[] s_index) throws IOException 
	{
		int off=get_dec(offset);
		ra.seek(off);
		String s1=ra.readLine();
		ra.seek(off);
		ra.writeBytes(s_index[0]+" "+s_index[1]+" "+String.format("%03d",Integer.parseInt(s_index[2]))+" "+offset_s2[1]+" "+"L"+" "+s_index[5]+" "+offset_s2[0]+" "+get_hex(off)+"\r\n");
		while(s1!=null)
		{
			off=off+add;
			String s_split[] = s1.split(" ");
			if(s_split[0].equals(s_index[0]) && s_split[1].equals(s_index[1]))
				s_split[2]=String.format("%03d", Integer.parseInt(s_split[2])+1);
			ra.seek(off);
			s1=ra.readLine();
			ra.seek(off);
			ra.writeBytes(s_split[0]+" "+s_split[1]+" "+s_split[2]+" "+s_split[3]+" "+s_split[4]+" "+s_split[5]+" "+s_split[6]+" "+get_hex(get_dec(s_split[7])+add)+"\r\n");
		}
		change_parent_offset();
	}
	
	/*
	 * Module Description: To modify the root nodes after insertion of a new leaf
	 */
	private static void change_parent_offset() throws IOException 
	{
		ra.seek(cur_data_offset);
		String loff=null;
		String[] s=ra.readLine().split(" ");
		String[] parents=get_parent_nodes("001");
		for(int i=0;parents[i]!=null;i=i+2)
		{
			String[] p=parents[i].split(" ");
			if(i==0)
				p[5]=get_first_node();
			else
			{
				String[] pp=parents[i-1].split(" ");
				p[5]=loff;	
			}
			p[6]=get_right_offset(p[3]);
			loff=p[6];
			ra.seek(get_dec(p[7]));
			ra.writeBytes(p[0]+" "+p[1]+" "+p[2]+" "+p[3]+" "+p[4]+" "+p[5]+" "+p[6]+" "+p[7]+"\r\n");
			if(parents[i+1]!=null)
			{
				p=parents[i+1].split(" ");
				p[5]=loff;
				p[6]=get_right_offset(p[3]);
				loff=p[6];
				ra.seek(get_dec(p[7]));
				ra.writeBytes(p[0]+" "+p[1]+" "+p[2]+" "+p[3]+" "+p[4]+" "+p[5]+" "+p[6]+" "+p[7]+"\r\n");
			}
		}
	}
	
	/*
	 * Module Description: To get the offset of the first leaf node in the index file
	 */
	private static String get_first_node() throws IOException 
	{
		ra.seek(cur_data_offset);
		String s=ra.readLine();
		while(s!=null)
		{
			String[] b=s.split(" ");
			if(b[0].equals("001"))
				return b[7];
			s=ra.readLine();
		}
		return null;
	}

	/*
	 * Module Description: To get the offset of the first right child of a root
	 */
	private static String get_right_offset(String key) throws IOException 
	{
		ra.seek(cur_data_offset);
		String s=ra.readLine();
		while(s!=null)
		{
			String[] b=s.split(" ");
			if(b[0].equals("001") && b[3].equals(key))
				return b[7];
			s=ra.readLine();
		}
		return null;
	}
	
	/*
	 * Module Description: To traverse the tree and get the offset of the element's block
	 */
	private static long traverse(String string) throws IOException 
	{
		ra.seek(cur_data_offset);
		String s=ra.readLine();
		String[] index_s = null;
		if(s!=null) index_s=s.split(" ");
		int flag=0,ii=0;
		String[] parents = null;
		if(!index_s[0].equals("001"))parents=get_parent_nodes(String.valueOf("001"));
		int off=cur_data_offset,i=0;
		while(s!=null)
		{
			flag=0;
			index_s=s.split(" ");
			if(index_s[0].equals("001"))
				return get_dec(index_s[7]);
			else
			{
				for(i=0;parents[i]!=null && flag==0;i++)
				{
					index_s=parents[i].split(" ");
					if(string.compareTo(index_s[3])>0){}
					else if(string.equals(index_s[3]))
					{
						off=get_dec(index_s[6]);
						ra.seek(off);
						s=ra.readLine();
						if(s!=null)index_s=s.split(" ");
							flag=1;
					}
					else 
					{
						off=get_dec(index_s[5]);
						ra.seek(off);
						s=ra.readLine();
						if(s!=null)index_s=s.split(" ");
						flag=1;
					}
				}
				if(flag==0&&i!=0)
				{
					index_s=parents[i-1].split(" ");
					off=get_dec(index_s[6]);
					s=ra.readLine();
				}
			}
			if(flag==1)
			{
				ra.seek(off);
				s=ra.readLine();
			}
		}
		return off;
	}
	
	/*
	 * Module Description: To get the offset of each record in the input file and insert it into a new file
	 */
	
	private static void get_key_offset(int key_size, String command, String rec,String test_data) throws IOException 
	{
		FileReader fr=new FileReader(new File(test_data));
		RandomAccessFile ra_test=new RandomAccessFile(test_data, "rw");
		RandomAccessFile ra_offset=new RandomAccessFile("offset.txt", "rw");
		BufferedWriter bw=new BufferedWriter(fw);
		BufferedReader br=new BufferedReader(fr);
		String key=null;
		char c = 0;
		long offset=0;
		if(command.equalsIgnoreCase("create"))
		{	
			ra_test.seek(0);
			ra_offset.seek(0);
		}
		else if(command.equalsIgnoreCase("insert"))
		{
			String new_rec[]=rec.split(" ");
			if(FIND(test_data,new_rec[0],"insert"))
			{
				System.out.println("Record already exists, not inserted.");
				return;
			}
			else
			{
				int len_test=(int) ra_test.length();
				ra_test.seek(len_test);
				ra_test.writeBytes("\r\n");
				int start=(int) ra_test.getFilePointer();
				ra_test.writeBytes(rec);
				ra_test.seek(0);
				ra_offset.seek(0);
			}
		}
		int i=0;
		String cur_offset=null;
		StringBuffer sb=new StringBuffer();
		try
		{
			offset=0;
			int l=0;
			while(l<ra_test.length())
			{
				if(i==0)
					offset=ra_test.getFilePointer();
				c=(char)ra_test.read();
				if(i<key_size)
					sb.append(c);
				i++;l++;
				if(i==key_size)
				{
					cur_offset=String.valueOf(Integer.toHexString((int) offset));
					cur_offset=left_padding(cur_offset);
					key=sb.toString(); 
					ra_offset.writeBytes(cur_offset+","+key+"\r\n");
					offset=0;
					sb.delete(0, sb.length());
				}
				if(c=='\n')i=0;
			}
		}	
		catch( IOException e)
		{
			System.out.println("IOException");
		}
		ra_offset.close();
		ra_test.close();
	}
	
	/*
	 * Module Description: To left pad a data with zeros 
	 */
	private static String left_padding(String str) 
	{
		int l=10-str.length();
		for (int i=0; i<l; i++) 
			str = "0" + str;
		return str;
	}
	
	/*
	 * Module Description: To right pad a data with zeros
	 */
	private static String right_padding(String key, String first_line_key) 
	{
		for (int i=key.length(); i<Integer.parseInt(first_line_key); i++) 
			key = key + " ";
		return key;
	}
}
