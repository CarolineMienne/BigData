import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MapReduce {
        /* Caroline MIENNE, ING5 app SI 1*/

        // Supposons que le fichier CSV utilise a pour separateur la virgule
        static String separateur = ",";

        public static class MyMapper extends Mapper<LongWritable, Text, IntWritable, KString>{

                @Override
                public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
                 {
                  	int idxColonne = 1;

                        //on decoupe la ligne en couples clef/valeur dont la cle est le numero de colonne
                        String[] contenus = value.toString().split(separateur);

                        if (contenus != null)
                        {
                                for (String contenu : contenus)
                                {
                                        context.write(new IntWritable(idxColonne), new KString(key.get(), contenu));
                                        idxColonne++;
                                }
                        }
                 }
        }

	public static class MyReducer extends Reducer<IntWritable,KString,IntWritable,Text>
        {
        /*
         * Le shuffle & sort genere pour chaque clef (ici numero de colonne) une liste de valeurs
         * */
                @Override
                public void reduce(IntWritable key, Iterable<KString> values, Context context) throws IOException, InterruptedException
                {
                        String ligneOutput = "";

                        // Comme rien ne nous assure que les lignes auront ete traitees dans l'ordre,
                        // on refait un tri
                        ArrayList<KString> sorted = new ArrayList<KString>();
                        for (KString obj : values)
                        {
                               	sorted.add(new KString(obj.key, obj.string));
                       	}

                        Collections.sort(sorted);

                       	// On reconstitue les nouvelles lignes (anciennes colonnes) a ecrire
                        Iterator<KString> iterator = sorted.iterator();
                       	while (iterator.hasNext())
                        {
                                ligneOutput += iterator.next().string;
                               	if (iterator.hasNext())
                                {
                                        ligneOutput += separateur;
                                }

                        }
                        context.write(key, new Text(ligneOutput));
                }
        }

        public static void main(String[] args) throws Exception {
                Configuration conf = new Configuration();
                Job job = Job.getInstance(conf, "PIVOT");
                job.setJarByClass(MapReduce.class);
                job.setMapperClass(MyMapper.class);
                //job.setCombinerClass(MyReducer.class); //Le combiner n'est pas interessant ici
                job.setReducerClass(MyReducer.class);
                job.setMapOutputKeyClass(IntWritable.class); //ajout
                job.setMapOutputValueClass(KString.class); //ajout
                job.setOutputKeyClass(IntWritable.class);
                job.setOutputValueClass(Text.class);
                FileInputFormat.addInputPath(job, new Path(args[0]));
                FileOutputFormat.setOutputPath(job, new Path(args[1]));
                System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
}
