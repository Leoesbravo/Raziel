using Microsoft.AspNetCore.Mvc;

namespace WebApi.Controllers
{
    public class NumeroExtraidoController : Controller
    {
        [HttpGet]
        [Route("api/NumeroExtraido/Extract/{numero}")]
        public ActionResult Extract(int numero)
        {

            if (numero < 1 || numero > 100)
            {
                return BadRequest("El número debe estar entre 1 y 100.");
            }

            Dictionary<string, object> resultado = Negocio.Numero.Extract(numero);
            bool result = (bool)resultado["Resultado"];

            if(result)
            {
                int numeroFaltante = (int)resultado["NumeroFaltante"];
                return Ok($"El número faltante es: {numeroFaltante}");
            }
            else
            {
                string excepcion = (string)resultado["Excepcion"];
                return BadRequest(excepcion);
            }
        }
    }
}
