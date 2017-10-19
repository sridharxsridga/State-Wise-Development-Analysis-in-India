/*
 * Pig UDF to filter the districts which have reached 80% of objectives of BPL cards
 * 
 */

package FilterDistrictUdf;

import java.io.IOException;
import org.apache.pig.FilterFunc;
import org.apache.pig.data.Tuple;

public class FilterDistrict extends FilterFunc{

	@Override
	public Boolean exec(Tuple input) throws IOException {
//Convert the tuple input to string type
		String line = input.toString();
//Tokenize the string with ',' delimiter using split method
		String[] columns = line.split(",");
//Store the BPL objective column
		Double bplObjective = Double.parseDouble(columns[2]);
//Store the BPL performance column
		Double bplPerformance = Double.parseDouble(columns[10]);
//if performance is greater than or equal to 80 % return
		if(bplPerformance >= ( (bplObjective* 80)/100) )
			return true;
		else
			return false;
		
		
	}

}