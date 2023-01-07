package com.telebionica.validator.ann;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.FIELD;

@Target({FIELD}) 
@Retention(RetentionPolicy.RUNTIME) 
public @interface Unique {
        public String message() default "{javax.validation.constraints.Unique.message}";
	Class<?>[] groups() default {};
}
