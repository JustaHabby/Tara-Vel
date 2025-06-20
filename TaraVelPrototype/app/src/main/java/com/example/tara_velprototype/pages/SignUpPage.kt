package com.example.tara_velprototype.pages

import android.widget.Toast
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Spacer
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.height
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.filled.MoreVert
import androidx.compose.material3.AlertDialog
import androidx.compose.material3.Button
import androidx.compose.material3.DropdownMenu
import androidx.compose.material3.DropdownMenuItem
import androidx.compose.material3.Icon
import androidx.compose.material3.IconButton
import androidx.compose.material3.OutlinedTextField
import androidx.compose.material3.Text
import androidx.compose.material3.TextButton
import androidx.compose.runtime.Composable
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.getValue
import androidx.compose.runtime.livedata.observeAsState
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.text.input.PasswordVisualTransformation
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.sp
import androidx.navigation.NavController
import androidx.compose.material3.AlertDialog



import com.example.tara_velprototype.AuthViewModel

@Composable
fun SignUpPage(modifier: Modifier = Modifier, navController: NavController, authViewModel: AuthViewModel) {
    var email by remember { mutableStateOf("") }
    var password by remember { mutableStateOf("") }
    var fpassword by remember { mutableStateOf("") }
    var showDialog by remember { mutableStateOf(false) } // Dialog control

    val authState = authViewModel.authState.observeAsState()
    val context = LocalContext.current

    LaunchedEffect(authState.value) {
        when (authState.value) {
            is AuthViewModel.AuthState.Authenticated -> {
                Toast.makeText(context, "Account created successfully! Please log in.", Toast.LENGTH_SHORT).show()
                navController.navigate("home") {
                    popUpTo("signup") { inclusive = true } // This clears "signup" from backstack
                }
            }

            is AuthViewModel.AuthState.Error -> {
                Toast.makeText(context, (authState.value as AuthViewModel.AuthState.Error).message, Toast.LENGTH_SHORT).show()
            }
            else -> Unit
        }
    }
    Column(
        modifier = modifier.fillMaxSize(),
        verticalArrangement = Arrangement.Center,
        horizontalAlignment = Alignment.CenterHorizontally
    ) {
        Text(text = "Sign-In Page", fontSize = 32.sp)
        Spacer(modifier = Modifier.height(16.dp))

        OutlinedTextField(
            value = email,
            onValueChange = { email = it },
            label = { Text(text = "Email") }
        )
        Spacer(modifier = Modifier.height(8.dp))

        OutlinedTextField(
            value = password,
            onValueChange = { password = it },
            label = { Text(text = "Password") },
            visualTransformation = PasswordVisualTransformation()
        )
        Spacer(modifier = Modifier.height(8.dp))
        OutlinedTextField(
            value = fpassword,
            onValueChange = { fpassword = it },
            label = { Text(text = "Fill Password") },
            visualTransformation = PasswordVisualTransformation()
        )

        Spacer(modifier = Modifier.height(16.dp))

        Button(
            onClick = { showDialog = true },
            enabled = if(password == fpassword) true else false
        ) {
            Text(text = "Create Account")
        }



        Spacer(modifier = Modifier.height(8.dp))
        TextButton(onClick = { navController.navigate("login") }) {
            Text(text = "Already have an Account?, Login")
        }
    }

    // Confirmation Dialog
    if (showDialog) {
        AlertDialog(
            onDismissRequest = { showDialog = false },
            title = { Text("Confirm Password") },
            text = { Text("Are you sure you want to create an account with this password?") },
            confirmButton = {
                TextButton(
                    onClick = {
                        showDialog = false
                        authViewModel.signup(email, password)
                    }
                ) {
                    Text("Yes")
                }
            },
            dismissButton = {
                TextButton(
                    onClick = {
                        showDialog = false
                        password = "" // Clear password
                    }
                ) {
                    Text("No")
                }
            }
        )
    }
}
