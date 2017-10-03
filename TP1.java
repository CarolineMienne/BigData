import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;


public class TP1 {
	
	/* Caroline MIENNE, ING5 app SI 1*/
	
	// Supposons que le fichier CSV utilise a pour separateur la virgule
	static String separateur = ",";
	
	/*
	 * Contrairement a l'exemple vu en cours,
	 * notre reduce() n'a pas pour but de realiser une fonction d'aggregation,
	 * au contraire l'ordre des lignes a une importance :
	 * il faut donc garder une trace des clefs originales generees par l'input reader.
	 * */
	private class KString implements Comparable<KString>
	{
		public Integer key;
		public String string;
		
		public KString (int key, String string)
		{
			this.key = key;
			this.string = string;
		}
		
		@Override
		public int compareTo(KString other)
		{
			return this.key.compareTo(other.key);
		}
	}
	
	/*
	 * On suppose qu'au prealable, l'Input reader a decoupe le fichier ligne par ligne
	 * */
	public HashMap<Integer, KString> map(int key, String value)
	{
		HashMap<Integer, KString> mapping = new HashMap<>();
		int idxColonne = 1;
		
		//on decoupe la ligne en couples clef/valeur dont la cle est le numero de colonne
		String[] contenus = value.split(separateur);
		
		for (String contenu : contenus)
		{
			mapping.put(idxColonne, new KString(key, contenu));
			idxColonne++;
		}
		
		return mapping;
	}
	
	
	/*
	 * Le shuffle & sort genere pour chaque clef (ici numero de colonne) une liste de valeurs
	 * */
	public String reduce (int key, ArrayList<KString> values)
	{
		String ligneOutput = "";
		
		// Comme rien ne nous assure que les lignes auront ete traitees dans l'ordre,
		// on refait un tri
		Collections.sort(values);
		
		// On reconstitue les nouvelles lignes (anciennes colonnes) a ecrire
		while (values.iterator().hasNext())
		{
			ligneOutput += values.iterator().next();
			if (values.iterator().hasNext())
			{
				ligneOutput += separateur;
			}
		}
		
		return ligneOutput;
	}
}
