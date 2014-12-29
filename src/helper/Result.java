package helper;

import java.util.Map.Entry;
import java.util.TreeMap;

public class Result {

	private TreeMap<String, Double> cycle_times = new TreeMap<String, Double>();
	private TreeMap<String, Double> inserts_per_second = new TreeMap<String, Double>();
	private TreeMap<String, Double> duration_per_insert = new TreeMap<String, Double>();

	public void addMeasureResult(String name, Double duration, Integer inserts) {
		this.cycle_times.put(name, duration);
		this.inserts_per_second.put(name, (inserts / duration));
		this.duration_per_insert.put(name, (duration / inserts));
	}

	public void printMeasureResult() {

		Double sum_duration = 0.0;
		Double sum_inserts_per_second = 0.0;
		Double sum_duration_per_insert = 0.0;
		Double average_duration = 0.0;
		Double average_inserts_per_second = 0.0;
		Double average_duration_per_insert = 0.0;
		Integer counter = 0;

		for (Entry<String, Double> cycle_time : this.cycle_times.entrySet()) {
			String name = cycle_time.getKey();
			Double duration = cycle_time.getValue();
			Double inserts_per_second = this.inserts_per_second.get(name);
			Double duration_per_insert = this.duration_per_insert.get(name);

			sum_duration += duration;
			sum_inserts_per_second += inserts_per_second;
			sum_duration_per_insert += duration_per_insert;
			counter++;

			System.out.println("Result for " + name + ": Duration = "
					+ round(duration, 2) + " Seconds / Inserts per Second = "
					+ round(inserts_per_second, 2) + " Average Time per Insert = " 
					+ round(duration_per_insert * 1000 , 2) + " ms");
		}

		average_duration = sum_duration / counter;
		average_inserts_per_second = sum_inserts_per_second / counter;
		average_duration_per_insert = sum_duration_per_insert / counter;
		
		System.out.println("Result for Average: Duration = " + round(average_duration, 2)
				+ " Seconds / Inserts per Second = " + round(average_inserts_per_second, 2) + 
				" Average Time per Insert = " + round(average_duration_per_insert * 1000, 2) + " ms");
	}
	
	private static double round(double value, int places) {
	    if (places < 0) throw new IllegalArgumentException();

	    long factor = (long) Math.pow(10, places);
	    value = value * factor;
	    long tmp = Math.round(value);
	    return (double) tmp / factor;
	}	
}
