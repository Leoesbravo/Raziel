using Microsoft.AspNetCore.Mvc;

namespace Presentacion.Controllers
{
    public class NumeroFaltanteController : Controller
    {
        public IActionResult CalcularNumero()
        {
            return View();
        }
    }
}
