package com.example.tara_velprototype

import androidx.compose.runtime.Composable
import androidx.compose.ui.Modifier
import androidx.navigation.compose.NavHost
import androidx.navigation.compose.composable
import androidx.navigation.compose.rememberNavController
import com.example.tara_velprototype.pages.HomePage
import com.example.tara_velprototype.pages.LoginPage
import com.example.tara_velprototype.pages.SignUpPage
import com.example.tara_velprototype.pages.UpdatePage

@Composable
fun MyAppNavigation(modifier: Modifier = Modifier, authViewModel: AuthViewModel) {
val navController = rememberNavController()

    NavHost(navController = navController, startDestination = "login", builder =  {
 composable(route = "login") {
     LoginPage(modifier,navController,authViewModel)
 }
        composable(route = "signup") {
            SignUpPage(modifier,navController,authViewModel)
        }
        composable(route = "home") {
            HomePage(modifier,navController,authViewModel)
        }
        composable("update") {
            UpdatePage(navController, authViewModel)
        }



})
}
