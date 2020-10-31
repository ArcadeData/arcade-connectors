package com.arcadeanalytics.provider.rdbms.nameresolver;

import static org.apache.commons.lang3.StringUtils.SPACE;

import com.arcadeanalytics.provider.rdbms.model.dbschema.CanonicalRelationship;
import java.util.Locale;
import org.apache.commons.lang3.StringUtils;

/**
 * Implementation of NameResolver that performs name transformations on the elements
 * of the data source according to the Java convention.
 *
 * @author Gabriele Ponzi
 */

public class JavaConventionNameResolver implements NameResolver {

  @Override
  public String resolveVertexName(String candidateName) {
    if (isCompliantToJavaClassConvention(candidateName)) {
      return candidateName;
    } else {
      candidateName = toJavaClassConvention(candidateName);

      return candidateName;
    }
  }

  @Override
  public String resolveVertexProperty(String candidateName) {
    if (isCompliantToJavaVariableConvention(candidateName)) {
      return candidateName;
    } else {
      candidateName = this.toJavaVariableConvention(candidateName);

      return candidateName;
    }
  }

  @Override
  public String resolveEdgeName(CanonicalRelationship relationship) {
    String finalName;

    // Foreign Key composed of 1 attribute
    if (relationship.getFromColumns().size() == 1) {
      String columnName = relationship.getFromColumns().get(0).getName();
      columnName = columnName.replace("_id", "");
      columnName = columnName.replace("_ID", "");
      columnName = columnName.replace("_oid", "");
      columnName = columnName.replace("_OID", "");
      columnName = columnName.replace("_eid", "");
      columnName = columnName.replace("_EID", "");

      if (!this.isCompliantToJavaClassConvention(columnName)) {
        // manipulating name (Java Convention)
        columnName = this.toJavaClassConvention(columnName);
      }

      finalName = "Has" + columnName;
    }
    // Foreign Key composed of multiple attribute
    else {
      finalName =
        this.toJavaClassConvention(relationship.getForeignEntity().getName()) + "2" + this.toJavaClassConvention(relationship.getParentEntity().getName());
    }

    return finalName;
  }

  public String toJavaClassConvention(String name) {
    // if all chars are uppercase, then name is transformed in a lowercase version

    boolean allUpperCase = true;
    for (int i = 0; i < name.length(); i++) {
      if (Character.isLowerCase(name.charAt(i))) {
        allUpperCase = false;
        break;
      }
    }

    if (allUpperCase) {
      name = name.toLowerCase(Locale.ENGLISH);
    }

    if (name.contains(" ")) {
      int pos;
      while (name.contains(" ")) {
        pos = name.indexOf(" ");
        name = name.substring(0, pos) + (name.charAt(pos + 1) + "").toUpperCase(Locale.ENGLISH) + name.substring(pos + 2);
      }
    }

    if (name.contains("_")) {
      int pos;
      while (name.contains("_")) {
        pos = name.indexOf("_");
        if (pos < name.length() - 1) {
          // the '_' char is not in last position
          name = name.substring(0, pos) + (name.charAt(pos + 1) + "").toUpperCase(Locale.ENGLISH) + name.substring(pos + 2);
        } else {
          // the '_' char is in last position
          name = name.substring(0, name.length() - 1);
        }
      }
    }

    if (name.contains("-")) {
      int pos;
      while (name.contains("-")) {
        pos = name.indexOf("-");
        name = name.substring(0, pos) + (name.charAt(pos + 1) + "").toUpperCase(Locale.ENGLISH) + name.substring(pos + 2);
      }
    }

    // First char must be uppercase
    if (Character.isLowerCase(name.charAt(0))) name = name.substring(0, 1).toUpperCase(Locale.ENGLISH) + name.substring(1);

    return name;
  }

  public String toJavaVariableConvention(String name) {
    // if all chars are uppercase, then name is transformed in a lowercase version

    boolean allUpperCase = true;
    for (int i = 0; i < name.length(); i++) {
      if (Character.isLowerCase(name.charAt(i))) {
        allUpperCase = false;
        break;
      }
    }

    if (allUpperCase) {
      name = name.toLowerCase(Locale.ENGLISH);
    }

    if (name.contains(SPACE)) {
      int pos;
      while (name.contains(SPACE)) {
        pos = name.indexOf(SPACE);
        name = name.substring(0, pos) + (name.charAt(pos + 1) + "").toUpperCase(Locale.ENGLISH) + name.substring(pos + 2);
      }
    }

    if (name.contains("_")) {
      int pos;
      while (name.contains("_")) {
        pos = name.indexOf("_");
        name = name.substring(0, pos) + (name.charAt(pos + 1) + "").toUpperCase(Locale.ENGLISH) + name.substring(pos + 2);
      }
    }

    if (name.contains("-")) {
      int pos;
      while (name.contains("-")) {
        pos = name.indexOf("-");
        name = name.substring(0, pos) + (name.charAt(pos + 1) + "").toUpperCase(Locale.ENGLISH) + name.substring(pos + 2);
      }
    }

    // First char must be lowercase
    if (Character.isUpperCase(name.charAt(0))) name = name.substring(0, 1).toLowerCase(Locale.ENGLISH) + name.substring(1);

    return name;
  }

  public boolean isCompliantToJavaClassConvention(String candidateName) {
    if (!(candidateName.contains(" ") || candidateName.contains("_") || candidateName.contains("-")) && Character.isUpperCase(candidateName.charAt(0))) {
      if (StringUtils.isAllUpperCase(candidateName)) {
        return false;
      }

      return true;
    }
    return false;
  }

  public boolean isCompliantToJavaVariableConvention(String candidateName) {
    if (
      !(candidateName.contains(" ") || candidateName.contains("_") || candidateName.contains("-")) && Character.isLowerCase(candidateName.charAt(0))
    ) return true;

    return false;
  }
}
