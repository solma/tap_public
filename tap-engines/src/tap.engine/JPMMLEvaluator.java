package tap.engine;

import org.jpmml.evaluator.ModelEvaluator;
import org.jpmml.evaluator.ModelEvaluatorFactory;
import org.jpmml.manager.ModelManagerFactory;
import org.jpmml.manager.PMMLManager;
import org.jpmml.model.ImportFilter;
import org.jpmml.model.JAXBUtil;
import org.dmg.pmml.PMML;
import org.xml.sax.InputSource;
import javax.xml.transform.Source;
import java.io.InputStream;
import java.io.FileInputStream;

public class JPMMLEvaluator {
  Evaluator evaluator;

  public JPMMLEvaluator(String pmmlS) {
    PMML pmml = null;
    // StringReader sr = new StringReader(pmmlS);

    try {
      InputStream sr = new FileInputStream(pmmlS);
      Source source = ImportFilter.apply(new InputSource(sr));
      pmml = JAXBUtil.unmarshalPMML(source);
    }
    catch (Exception e) {
      e.printStackTrace(System.out);
    }
    finally {
    }

    PMMLManager pmmlManager = new PMMLManager(pmml);
    ModelManagerFactory modelManagerFactory = ModelEvaluatorFactory.getInstance();
    ModelEvaluator<?> modelEvaluator = (ModelEvaluator<?>)pmmlManager.getModelManager(modelManagerFactory);
    // Evaluator evaluator = (Evaluator)modelEvaluator;
  }

}
