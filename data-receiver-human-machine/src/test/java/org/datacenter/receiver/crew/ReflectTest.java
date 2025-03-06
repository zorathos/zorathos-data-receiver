package org.datacenter.receiver.crew;

    import org.datacenter.model.crew.PersonnelInfo;
    import org.junit.jupiter.api.Test;

    import java.lang.reflect.Method;
    import java.sql.PreparedStatement;

    /**
     * \@author : [wangminan]
     * \@description : 反射测试
     */
    public class ReflectTest {

        @Test
        void testMain() {
            String columnName = "id";
            String columnJdbcType = "String";

            PersonnelInfo personnelInfo = PersonnelInfo.builder()
                    .id("123")
                    .name("wangminan")
                    .build();
            // 从personnelInfo反射调用getId 打印
            try {
                Method getMethod = PersonnelInfo.class
                        .getMethod("get" + columnName.substring(0, 1).toUpperCase() + columnName.substring(1));
                Object result = getMethod.invoke(personnelInfo);
                System.out.println(result);

                // 动态加载参数类型
                Class<?> parameterType;
                switch (columnJdbcType) {
                    case "String":
                        parameterType = String.class;
                        break;
                    case "Date":
                        parameterType = java.sql.Date.class;
                        break;
                    // 其他类型可以在这里添加
                    default:
                        throw new IllegalArgumentException("Unsupported columnJdbcType: " + columnJdbcType);
                }

                // 反射出setColumnJdbcType的方法 并且填入参数类型 1. int.class ColumnJdbcType.class
                Method setMethod = PreparedStatement.class
                        .getMethod("set" + columnJdbcType, int.class, parameterType);
                // 示例调用
                PreparedStatement preparedStatement = null; // 这里需要一个实际的PreparedStatement对象
                setMethod.invoke(preparedStatement, 1, result);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
