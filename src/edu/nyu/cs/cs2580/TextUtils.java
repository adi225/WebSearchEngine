package edu.nyu.cs.cs2580;

import java.text.Normalizer;
import java.util.regex.Pattern;

import org.xml.sax.SAXException;

import de.l3s.boilerpipe.BoilerpipeProcessingException;
import de.l3s.boilerpipe.document.TextDocument;
import de.l3s.boilerpipe.extractors.ArticleExtractor;
import de.l3s.boilerpipe.sax.BoilerpipeSAXInput;
import de.l3s.boilerpipe.sax.HTMLDocument;

public class TextUtils {

  // Non-visible page content is removed, e.g., those inside <script> tags.
  // Right now, the 3rd party library "BoilerPiper" is used to perform the task.
  public static String removeNonVisibleContext(Document document, String text) throws BoilerpipeProcessingException, SAXException {
    HTMLDocument htmlDoc = new HTMLDocument(text);
    BoilerpipeSAXInput boilerpipeSaxInput = new BoilerpipeSAXInput(htmlDoc.toInputSource());
    TextDocument doc = boilerpipeSaxInput.getTextDocument();
    document.setTitle(doc.getTitle());
    return ArticleExtractor.INSTANCE.getText(text);
  }

  public static String removePunctuation(String text, String ignoreCharacterClass) {
    return text.replaceAll("[^a-zA-Z0-9" + ignoreCharacterClass + "\n]", " ");
  }
  
  // Tokens are stemmed with Step 1 of the Porter's algorithm.
  public static String performStemming(String text){
    Stemmer stemmer = new Stemmer();
    stemmer.add(text.toCharArray(), text.length());
    stemmer.stem();
	return stemmer.toString();
  }

  public static String removeInitialsDots(String str) {
    str=str.replaceAll("(?i)(^([a-z])\\.|(?<= )([a-z])\\.|(?<=\\.)([a-z])\\.)", "$2$3$4").trim();
    str=str.replaceAll("(?i)^(([a-z]) ([a-z]))($| )", "$2$3"+" ").trim();
    str=str.replaceAll("(?i)(?<= )(([a-z]) ([a-z]))($| )", "$2$3"+" ").trim();
    return str;
  }

  public static String deAccent(String str) {
    String nfdNormalizedString = Normalizer.normalize(str, Normalizer.Form.NFD);
    Pattern pattern = Pattern.compile("\\p{InCombiningDiacriticalMarks}+");
    return pattern.matcher(nfdNormalizedString).replaceAll("");
  }

  public static String convertUnicodeSpecialLettersToASCII(String str) {
    StringBuilder result = new StringBuilder();
    for(int i = 0; i < str.length(); i++) {
      int unicodeCodePoint = str.codePointAt(i);
      int charType = Character.getType(unicodeCodePoint);
      if(charType == Character.OTHER_LETTER) {
        result.append(" U" + unicodeCodePoint + " ");
      } else {
        result.append(str.charAt(i));
      }
    }
    return result.toString();
  }
}
