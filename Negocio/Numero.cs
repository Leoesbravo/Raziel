using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Negocio
{
    public class Numero
    {
        public static Dictionary<string, object> Extract(int numero)
        {
            int numeroFaltante = 0;
            Dictionary<string, object> diccionario = new Dictionary<string, object> { { "Resultado", false }, { "NumeroFaltante", numeroFaltante }, { "Excepcion", "" } };
            try
            {
                List<int> numerosNaturales = Enumerable.Range(1, 100).ToList();

                if (numero < 1 || numero > 100)
                {
                    Console.WriteLine("El número ingresado está fuera del rango válido.");
                }
                else if (!numerosNaturales.Contains(numero))
                {
                    Console.WriteLine("El número ingresado no es un número natural válido.");
                }
                else
                {
                    numerosNaturales.Remove(numero);

                    foreach (int num in Enumerable.Range(1, 100))
                    {
                        if (!numerosNaturales.Contains(num))
                        {
                            numeroFaltante = num;
                            break;
                        }
                    }
                    diccionario["NumeroFaltante"] = numeroFaltante;
                    diccionario["Resultado"] = true;
                }
            }
            catch (Exception e)
            {
                diccionario["Excepcion"] = e.Message;
            }
            return diccionario;
        }
    }
}
