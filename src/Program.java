import java.io.IOException;
import org.apache.hadoop.mapreduce.Job;

public class Program {

	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException, Exception
	{
		String input = args[0]; // input file
		String output = args[1]; // output folder
		int k = Integer.parseInt(args[2]); // number of clusters
		Double tol = Double.parseDouble(args[3]); // desired accuracy
		String algo = args[4]; // algorithm choice
		if(algo.toLowerCase().equals(new String("kmeans")))
		{
			ClusteringInitializer.SetupJob(input, output+"0", k).waitForCompletion(true);
		}
		else if(algo.toLowerCase().equals(new String("kmeans++")))
		{
			ClusteringInitializer.SetupJob(input, output+".init0", 1).waitForCompletion(true);
			
			for(Integer i = 1; i < k; i++)
			{
				String initOutput = output + (i == k - 1 ? "0":".init" + i.toString());
				KMeansPPUpdate.SetupJob(input, output+".init" + ((Integer)(i-1)).toString(), initOutput).waitForCompletion(true);
			}
		}
		else
		{
			System.out.println("Wrong algorithm choice!!!" + algo.toLowerCase());
			System.exit(-1);
		}
		Long counter = 1L;// # of not convergent reducers
		Integer i = 0; // folders enumerator
		
		while(counter != 0)
		{
			i++;
			Job job = KMeansUpdate.SetupJob(input, output+((Integer)(i-1)).toString(), output+i.toString(), tol);
			job.waitForCompletion(true);
			counter = job.getCounters().findCounter(KMeansUpdate.Counter.CONVERGED).getValue();
		}
		
		KMeansResult.SetupJob(input, output+i.toString(), output).waitForCompletion(true);
	}
}
