package model.schema;

import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.util.ArrayList;
import java.util.List;

import static GatesBigData.constants.XmlConfig.*;
import static GatesBigData.utils.Utils.*;

public class XmlProperty {
    String name  = null;
    String value = null;
    List<XmlProperty> childProperties;
    boolean valid = true;

    public XmlProperty(Node n) {
        String tag = n.getNodeName();
        this.valid = isPropertyTag(tag);

        if (this.valid) {
            initialize(n.getChildNodes());
        }
    }

    private void initialize(NodeList n) {
        for(int i = 0; i < n.getLength(); i++) {
            String tag = n.item(i).getNodeName();
            if (isNameTag(tag)) {
                this.name = n.item(i).getTextContent();
            } else if (isValueTag(tag)) {
                this.value = n.item(i).getTextContent().trim();

                if (n.item(i).hasChildNodes()) {
                    this.childProperties = getXmlProperties(n.item(i).getChildNodes());
                }
            }
        }
    }

    public boolean isValid() {
        return this.valid;
    }

    public List<XmlProperty> getChildProperties() {
        return this.childProperties;
    }

    public String getName() {
        return this.name;
    }

    public String getValue() {
        return this.value;
    }

    public List<String> getValueList() {
        List<String> vals = new ArrayList<String>();
        for(String val : getTokens(this.value, ",")) {
            vals.add(val.trim());
        }
        return vals;
    }

    public boolean hasNameAndValue() {
        return this.name != null && this.value != null;
    }
}
