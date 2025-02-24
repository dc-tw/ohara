/*
 * Copyright 2019 is-land
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.island.ohara

import java.io.FileInputStream
import java.util.jar.JarInputStream
import java.util.regex.Pattern

import org.junit.Test

package object assembly {

  /**
    * return the super class and interfaces of input class.
    * @param clz input class
    * @return super class and interfaces
    */
  def superClasses(clz: Class[_]): Seq[Class[_]] = new Iterator[Class[_]] {
    private[this] var current = clz.getSuperclass
    override def hasNext: Boolean = current != null
    override def next(): Class[_] = try current
    finally current = current.getSuperclass
  }.toSeq ++ clz.getInterfaces

  /**
    * seek the methods having annotation "Test" from a class
    * @param clz class
    * @return methods having annotation "Test"
    */
  def testCases(clz: Class[_]): Set[String] = clz.getMethods
    .filter { m =>
      val annotations = m.getAnnotations
      if (annotations == null || annotations.isEmpty) false
      else annotations.exists(_.annotationType() == classOf[Test])
    }
    .map(_.getName)
    .toSet

  /**
    * Java generate $number class for the class which has private constructor and we don't care for them in some tests.
    * This helper method is used to filter them
    * from our tests
    * @param clz class
    * @return true if the input class is anonymous.
    */
  def isAnonymous(clz: Class[_]): Boolean = clz.getName.contains("$")

  /**
    * Find the test classes which have following attributes.
    * 1) in tests.jar
    * 2) the name starts with "Test"
    * 3) non anonymous class (It is impossible to use anonymous class to write ohara test)
    * @return test classes
    */
  def testClasses(): Seq[Class[_]] = allClasses(_.contains("tests.jar"))
  // the test class should not be anonymous class
    .filterNot(isAnonymous)
    // the previous filter had chosen the normal class so it is safe to produce the simple name for classes
    .filter(_.getSimpleName.startsWith("Test"))

  /**
    * @return all classes in testing scope
    */
  def classesInTestScope(): Seq[Class[_]] = allClasses(_.contains("tests.jar"))

  /**
    * @return all classes in production scope
    */
  def classesInProductionScope(): Seq[Class[_]] = allClasses(n => !n.contains("tests.jar"))

  def allClasses(fileNameFilter: String => Boolean): Seq[Class[_]] = {
    val classLoader = ClassLoader.getSystemClassLoader
    val path = "com/island/ohara"
    val pattern = Pattern.compile("^file:(.+\\.jar)!/" + path + "$")
    val urls = classLoader.getResources(path)
    Iterator
      .continually(urls.nextElement())
      .takeWhile(_ => urls.hasMoreElements)
      .map(url => pattern.matcher(url.getFile))
      .filter(_.find())
      .map(_.group(1))
      .filter(fileNameFilter)
      .flatMap { f =>
        val jarInput = new JarInputStream(new FileInputStream(f))
        try Iterator
          .continually(jarInput.getNextJarEntry)
          .takeWhile(_ != null)
          .map(_.getName)
          .toArray
          .filter(_.endsWith(".class"))
          .map(_.replace('/', '.'))
          .map(className => className.substring(0, className.length - ".class".length))
          .map(Class.forName)
        finally jarInput.close()
      }
      .toSeq
  }
}
