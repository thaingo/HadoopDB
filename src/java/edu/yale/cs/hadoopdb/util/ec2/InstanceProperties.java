package edu.yale.cs.hadoopdb.util.ec2;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * Retrieves instance properties from EC2 for a given cluster. Default
 * behavior provides the set of instance ids in all owned EC2 clusters. 
 * The following command lines alter this behavior
 * -public: Provides a list of all public ips 
 * -private: Provides a list of all internal ips
 * -group group_name: Provides the properties for a given group
 */
public class InstanceProperties {

	public static void main(String[] args) {
		int out_index = 1; //default is instance id
		String group_name = "";
		boolean test_group = false;
	    for(int i = 0; i < args.length; i++){
	    	if("-public".equals(args[i])){
	    		out_index = 3;
	    	}
	    	else if("-private".equals(args[i])){
	    		out_index = 4;
	    	}
	    	else if("-group".equals(args[i])){
	    		group_name = args[++i];
	    		test_group = true;
	    	}
	    }
	    
		BufferedReader in = new BufferedReader(new InputStreamReader(System.in));   
	    String line = "";
	    try {
	    	boolean ingroup = false;
			while((line = in.readLine())!= null){
				String[] fields = line.split("(\\s)+");
				boolean running = false; 
				if(fields[0].equals("RESERVATION")){
					ingroup = false;
					if(test_group){
						for(String field : fields){
							if(group_name.equals(field))
								ingroup = true;
						}
					}
				}
				if(fields[0].equals("INSTANCE")){
					for(String field : fields){
						if("running".equals(field))
							running = true;
					}
					if(running && 
							((test_group && ingroup) || !test_group))
						System.out.println(fields[out_index]);
				}
			}
		}
		catch (IOException e) {
			e.printStackTrace();
		}

	}

}
